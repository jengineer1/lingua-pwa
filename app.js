/* Lingua PWA
 *
 * Offline reader for lesson packages built by the ingest pipeline.
 * No dependencies: the zip reader below uses DecompressionStream, and
 * storage is plain IndexedDB.
 *
 * Two stores matter conceptually:
 *   lessons/media - imported content, read-only once written
 *   events        - append-only log of what you've marked and studied
 *
 * Vocabulary state is DERIVED by replaying events, never mutated in place.
 * That is what makes backup, restore and (later) multi-device sync work:
 * merging two devices is concatenating their logs, which cannot conflict
 * because no two devices ever write the same log.
 */

'use strict';

const DB_NAME = 'lingua';
const DB_VER = 1;
const DEVICE = (() => {
  let d = localStorage.getItem('lingua.device');
  if (!d) { d = 'd' + Math.random().toString(36).slice(2, 10); localStorage.setItem('lingua.device', d); }
  return d;
})();

/* ------------------------------------------------------------------ zip */
/* Minimal reader for the subset we produce: no zip64, no encryption.
   Media is stored uncompressed, everything else deflated. */

async function unzip(blob) {
  const buf = await blob.arrayBuffer();
  const dv = new DataView(buf);
  const n = buf.byteLength;

  let eocd = -1;
  for (let i = n - 22; i >= Math.max(0, n - 65558); i--) {
    if (dv.getUint32(i, true) === 0x06054b50) { eocd = i; break; }
  }
  if (eocd < 0) throw new Error('Not a valid package file');

  const count = dv.getUint16(eocd + 10, true);
  let off = dv.getUint32(eocd + 16, true);
  const dec = new TextDecoder();
  const out = {};

  for (let i = 0; i < count; i++) {
    if (dv.getUint32(off, true) !== 0x02014b50) throw new Error('Damaged package');
    const method = dv.getUint16(off + 10, true);
    const csize = dv.getUint32(off + 20, true);
    const nameLen = dv.getUint16(off + 28, true);
    const extraLen = dv.getUint16(off + 30, true);
    const cmtLen = dv.getUint16(off + 32, true);
    const local = dv.getUint32(off + 42, true);
    const name = dec.decode(new Uint8Array(buf, off + 46, nameLen));

    const lNameLen = dv.getUint16(local + 26, true);
    const lExtraLen = dv.getUint16(local + 28, true);
    const start = local + 30 + lNameLen + lExtraLen;

    out[name] = { method, raw: buf.slice(start, start + csize) };
    off += 46 + nameLen + extraLen + cmtLen;
  }
  return out;
}

async function entryBlob(e, type) {
  if (e.method === 0) return new Blob([e.raw], { type });
  const s = new Blob([e.raw]).stream()
    .pipeThrough(new DecompressionStream('deflate-raw'));
  const b = await new Response(s).blob();
  return type ? new Blob([b], { type }) : b;
}

const entryText = async e => await (await entryBlob(e)).text();

/* -------------------------------------------------------------- storage */

function openDB() {
  return new Promise((res, rej) => {
    const r = indexedDB.open(DB_NAME, DB_VER);
    r.onupgradeneeded = () => {
      const db = r.result;
      db.createObjectStore('lessons', { keyPath: 'id' });
      db.createObjectStore('media', { keyPath: 'id' });
      const ev = db.createObjectStore('events', { keyPath: 'seq', autoIncrement: true });
      ev.createIndex('ts', 'ts');
    };
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
}

let db = null;

function tx(store, mode = 'readonly') {
  return db.transaction(store, mode).objectStore(store);
}
function req(r) {
  return new Promise((res, rej) => {
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
}

/* ------------------------------------------------------------ app state */

const S = {
  lessons: [],      // manifests
  vocab: {},        // 'es:lemma' -> status
  studied: {},      // lessonId -> seconds
  doc: null,        // current lesson content
  man: null,        // current manifest
  segs: [],
  flat: [],
  curSeg: -1,
  curTok: -1,
  media: null,
  loop: false,
  rate: 1,
  tick: null,
  lastCount: 0,
};

const $ = id => document.getElementById(id);
const esc = s => (s || '').replace(/[&<>"]/g, c =>
  ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c]));

function toast(msg, ms = 2200) {
  const t = $('toast');
  t.textContent = msg;
  t.classList.add('up');
  clearTimeout(t._t);
  t._t = setTimeout(() => t.classList.remove('up'), ms);
}

/* ------------------------------------------------------------- events */

async function emit(kind, payload) {
  const ev = { device: DEVICE, ts: Date.now(), kind, ...payload };
  await req(tx('events', 'readwrite').add(ev));
  applyEvent(ev);
}

function applyEvent(ev) {
  if (ev.kind === 'status') S.vocab[ev.key] = ev.value;
  else if (ev.kind === 'study') S.studied[ev.lesson] = (S.studied[ev.lesson] || 0) + ev.value;
}

async function replayEvents() {
  S.vocab = {}; S.studied = {};
  const all = await req(tx('events').getAll());
  all.sort((a, b) => a.ts - b.ts);   // last write per key wins
  all.forEach(applyEvent);
}

/* ------------------------------------------------------------- import */

async function importFile(file) {
  toast('Reading ' + file.name + '…', 60000);
  const files = await unzip(file);

  if (!files['manifest.json']) throw new Error('Missing manifest — not a lesson package');
  const man = JSON.parse(await entryText(files['manifest.json']));

  const jsonEntry = files['lesson.json'];
  if (!jsonEntry) {
    throw new Error('This package predates lesson.json. Re-run package.py.');
  }
  const doc = JSON.parse(await entryText(jsonEntry));

  const mediaEntry = files[man.media];
  if (!mediaEntry) throw new Error('Missing media in package');
  const type = man.media.endsWith('.m4a') ? 'audio/mp4' : 'video/mp4';
  const blob = await entryBlob(mediaEntry, type);

  await req(tx('lessons', 'readwrite').put({ id: man.id, man, doc }));
  await req(tx('media', 'readwrite').put({ id: man.id, blob }));

  toast('Imported ' + man.title);
  await loadLibrary();
}

/* ------------------------------------------------------------ library */

function lessonKnown(doc, lang) {
  const set = new Set();
  doc.segments.forEach(s => s.tokens.forEach(t => t.lemma && set.add(t.lemma)));
  let known = 0;
  set.forEach(l => {
    const st = S.vocab[lang + ':' + l];
    if (st === 'known' || st === 'ignore') known++;
  });
  return { total: set.size, known };
}

async function loadLibrary() {
  const rows = await req(tx('lessons').getAll());
  S.lessons = rows;
  renderLibrary();
}

function renderLibrary() {
  const list = $('list');
  const has = S.lessons.length > 0;
  $('empty').classList.toggle('hidden', has);
  $('import2').classList.toggle('hidden', !has);

  if (!has) { list.innerHTML = ''; return; }

  const groups = new Map();
  S.lessons.forEach(r => {
    const k = r.man.series || '';
    if (!groups.has(k)) groups.set(k, []);
    groups.get(k).push(r);
  });

  let html = '';
  const keys = [...groups.keys()].sort((a, b) => (a === '' ? -1 : b === '' ? 1 : a.localeCompare(b)));
  for (const k of keys) {
    const items = groups.get(k).sort((a, b) =>
      (a.man.chapter || 0) - (b.man.chapter || 0) ||
      a.man.title.localeCompare(b.man.title));
    if (k) html += `<div class="series">${esc(k)}</div>`;
    for (const r of items) {
      const { total, known } = lessonKnown(r.doc, r.man.language);
      const pct = total ? Math.round(known / total * 100) : 0;
      const mins = Math.round(r.man.duration / 60);
      const secs = S.studied[r.man.id] || 0;
      const done = secs > 30 ? ` · studied ${Math.round(secs / 60)}m` : '';
      html += `<div class="card" data-id="${r.man.id}">
        <div class="body">
          <h3>${esc(r.man.title)}</h3>
          <div class="meta">${r.man.kind} · ${mins} min · ${r.man.segments} sentences${done}</div>
          <div class="known">${pct}% known words (${known}/${total})</div>
        </div>
        <button class="del" data-del="${r.man.id}">×</button>
      </div>`;
    }
  }
  list.innerHTML = html;
}

/* ------------------------------------------------------------- player */

async function openLesson(id) {
  const row = await req(tx('lessons').get(id));
  const m = await req(tx('media').get(id));
  if (!row || !m) { toast('Lesson data missing'); return; }

  S.man = row.man; S.doc = row.doc; S.segs = row.doc.segments;
  S.curSeg = -1; S.curTok = -1; S.lastCount = 0;

  const stage = $('stage');
  stage.classList.toggle('audio', row.man.kind === 'audio');
  const tag = row.man.kind === 'audio' ? 'audio' : 'video';
  stage.innerHTML = `<${tag} id="v" controls playsinline preload="metadata"></${tag}>`;
  S.media = $('v');
  S.media.src = URL.createObjectURL(m.blob);

  buildFlat();
  renderScript();

  $('title').textContent = row.man.title;
  $('library').classList.add('hidden');
  $('player').classList.remove('hidden');
  $('back').classList.remove('hidden');
  $('size').classList.toggle('hidden', row.man.kind === 'audio');
  updateStat();

  S.media.addEventListener('play', startClock);
  S.media.addEventListener('pause', stopClock);
  S.media.addEventListener('ended', stopClock);
  if (S.tick) cancelAnimationFrame(S.tick);
  S.tick = requestAnimationFrame(loop);
}

function closeLesson() {
  stopClock();
  if (S.tick) cancelAnimationFrame(S.tick);
  S.tick = null;
  if (S.media) { S.media.pause(); URL.revokeObjectURL(S.media.src); }
  S.media = null; S.doc = null;
  $('stage').innerHTML = '';
  $('player').classList.add('hidden');
  $('library').classList.remove('hidden');
  $('back').classList.add('hidden');
  $('size').classList.add('hidden');
  $('title').textContent = 'Lingua';
  $('stat').textContent = '';
  renderLibrary();
}

function buildFlat() {
  S.flat = [];
  S.segs.forEach((s, si) => s.tokens.forEach((t, ti) => {
    if (t.start != null) S.flat.push({ si, ti, start: t.start, end: t.end });
  }));
  S.flat.sort((a, b) => a.start - b.start);
}

function statusOf(lemma) {
  if (!lemma) return 'new';
  return S.vocab[S.man.language + ':' + lemma] || 'new';
}

function renderScript() {
  const html = S.segs.map((s, si) => {
    let out = '', cur = 0;
    const phr = s.phrases || [];
    s.tokens.forEach((t, ti) => {
      const a = t.start_char, b = t.end_char;
      if (a > cur) out += esc(s.text.slice(cur, a));
      const inPh = phr.some(p => a >= p.start_char && b <= p.end_char);
      out += `<span class="w s-${statusOf(t.lemma)}${inPh ? ' ph' : ''}"`
           + ` data-si="${si}" data-ti="${ti}">${esc(s.text.slice(a, b))}</span>`;
      cur = b;
    });
    if (cur < s.text.length) out += esc(s.text.slice(cur));
    return `<div class="seg" id="g${si}" data-si="${si}">`
         + `<div class="txt">${out}</div>`
         + `<div class="tr">${esc(s.translation || '')}</div></div>`;
  }).join('');
  $('script').innerHTML = html;
}

function updateStat() {
  if (!S.doc) return;
  const { total, known } = lessonKnown(S.doc, S.man.language);
  $('stat').textContent = total ? Math.round(known / total * 100) + '%' : '';
}

/* --------------------------------------------------------- play clock */
/* Study time is accumulated in short bursts rather than one event per
   session, so a crash or a swipe-away loses at most a few seconds. */

function startClock() {
  if (S._clock) return;
  S._clock = setInterval(() => {
    if (S.media && !S.media.paused) emit('study', { lesson: S.man.id, value: 15 });
  }, 15000);
}
function stopClock() {
  clearInterval(S._clock);
  S._clock = null;
}

function loop() {
  S.tick = requestAnimationFrame(loop);
  if (!S.media) return;
  const t = S.media.currentTime;

  let si = -1;
  for (let i = 0; i < S.segs.length; i++) {
    const s = S.segs[i];
    if (s.start != null && t >= s.start - 0.05 && t <= s.end + 0.05) { si = i; break; }
  }

  if (si !== S.curSeg) {
    document.querySelectorAll('.seg.cur').forEach(e => e.classList.remove('cur'));
    document.querySelectorAll('.seg.peek').forEach(e => e.classList.remove('peek'));
    if (si >= 0) {
      const el = $('g' + si);
      el.classList.add('cur');
      const r = el.getBoundingClientRect();
      const sr = $('script').getBoundingClientRect();
      if (r.top < sr.top + 30 || r.bottom > sr.bottom - 100) {
        el.scrollIntoView({ block: 'center', behavior: 'smooth' });
      }
    }
    S.curSeg = si;
  }

  // Sticky: hold the last word that started until the next one does.
  let idx = -1;
  if (si >= 0) {
    let lo = 0, hi = S.flat.length - 1;
    while (lo <= hi) {
      const mid = (lo + hi) >> 1;
      if (S.flat[mid].start <= t) { idx = mid; lo = mid + 1; } else hi = mid - 1;
    }
    if (idx >= 0 && S.flat[idx].si !== si) idx = -1;
  }
  if (idx !== S.curTok) {
    document.querySelectorAll('.w.on').forEach(e => e.classList.remove('on'));
    if (idx >= 0) {
      const f = S.flat[idx];
      const el = document.querySelector(`.w[data-si="${f.si}"][data-ti="${f.ti}"]`);
      if (el) el.classList.add('on');
    }
    S.curTok = idx;
  }

  if (S.loop && S.curSeg >= 0 && t > S.segs[S.curSeg].end) {
    S.media.currentTime = S.segs[S.curSeg].start;
  }
}

function seek(t) {
  if (t == null || !S.media) return;
  S.media.currentTime = t;
  S.media.play().catch(() => {});
}
function goSeg(d) {
  let i = S.curSeg < 0 ? 0 : S.curSeg + d;
  i = Math.max(0, Math.min(S.segs.length - 1, i));
  seek(S.segs[i].start);
}

/* --------------------------------------------------------------- sheet */

function showSheet(html) {
  $('sheet').innerHTML = html;
  $('sheet').classList.add('up');
  $('scrim').classList.add('up');
}
function hideSheet() {
  $('sheet').classList.remove('up');
  $('scrim').classList.remove('up');
}

function showWord(si, ti) {
  const s = S.segs[si], t = s.tokens[ti];
  const lemma = t.lemma;
  const st = statusOf(lemma);
  const phrase = (s.phrases || []).find(p =>
    t.start_char >= p.start_char && t.end_char <= p.end_char);

  let h = `<div class="hw">${esc(t.article ? t.article + ' ' : '')}${esc(t.surface)}</div>`;
  if (lemma && lemma !== t.surface.toLowerCase()) {
    h += `<div class="lm">${esc(lemma)}${t.pos ? ' · ' + t.pos.toLowerCase() : ''}</div>`;
  }
  if (t.gloss_override) {
    h += `<div class="ov">here: ${esc(t.gloss_override)}</div>`;
    if (t.override_note) h += `<div class="nt">${esc(t.override_note)}</div>`;
  }
  h += `<div class="gl">${esc(t.gloss || '(no definition)')}</div>`;
  if (t.note) h += `<div class="nt">${esc(t.note)}</div>`;
  if (phrase) {
    h += `<div class="ov">${esc(phrase.surface)} — ${esc(phrase.gloss || '')}</div>`;
    if (phrase.note) h += `<div class="nt">${esc(phrase.note)}</div>`;
  }
  if (lemma) {
    h += '<div class="row">' + ['new', 'learning', 'known', 'ignore'].map(k =>
      `<button data-set="${k}" class="${st === k ? 'on' : ''}">${k}</button>`).join('') + '</div>';
  }
  showSheet(h);

  $('sheet').querySelectorAll('[data-set]').forEach(b => b.onclick = async () => {
    await emit('status', { key: S.man.language + ':' + lemma, value: b.dataset.set });
    document.querySelectorAll('.w').forEach(e => {
      const tk = S.segs[+e.dataset.si].tokens[+e.dataset.ti];
      if (tk.lemma === lemma) e.className = e.className.replace(/s-\w+/, 's-' + b.dataset.set);
    });
    updateStat();
    hideSheet();
  });
}

/* -------------------------------------------------------------- backup */

async function exportBackup() {
  const events = await req(tx('events').getAll());
  const lessons = (await req(tx('lessons').getAll())).map(r => ({
    id: r.man.id, title: r.man.title, language: r.man.language,
  }));
  const blob = new Blob([JSON.stringify({
    format: 1, exported: new Date().toISOString(), device: DEVICE,
    lessons, events,
  }, null, 1)], { type: 'application/json' });

  const name = 'lingua-backup-' + new Date().toISOString().slice(0, 10) + '.json';
  const file = new File([blob], name, { type: 'application/json' });

  // Share sheet on iOS puts it straight into Files or iCloud Drive.
  if (navigator.canShare && navigator.canShare({ files: [file] })) {
    try { await navigator.share({ files: [file], title: name }); return; } catch (e) { /* cancelled */ }
  }
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}

async function importBackup(file) {
  const data = JSON.parse(await file.text());
  if (!data.events) throw new Error('Not a Lingua backup');
  const store = tx('events', 'readwrite');
  // Events are idempotent by (device, ts, kind, key): re-importing the same
  // backup twice cannot double-count, because status is last-write-wins and
  // study events carry their own timestamps.
  const seen = new Set((await req(tx('events').getAll()))
    .map(e => e.device + '|' + e.ts + '|' + e.kind + '|' + (e.key || e.lesson)));
  let added = 0;
  for (const e of data.events) {
    const k = e.device + '|' + e.ts + '|' + e.kind + '|' + (e.key || e.lesson);
    if (seen.has(k)) continue;
    const { seq, ...rest } = e;
    store.add(rest); added++;
  }
  await replayEvents();
  renderLibrary(); updateStat();
  toast(`Merged ${added} events`);
}

/* ---------------------------------------------------------------- menu */

function showMenu() {
  const lang = S.man ? S.man.language : 'es';
  const known = Object.entries(S.vocab)
    .filter(([k, v]) => k.startsWith(lang + ':') && v === 'known').length;
  const total = Math.round(Object.values(S.studied).reduce((a, b) => a + b, 0) / 60);
  showSheet(`
    <div class="hw">Lingua</div>
    <div class="lm">${known} words known · ${total} minutes studied</div>
    <div class="row" style="flex-direction:column">
      <button data-act="import">Import lesson (.lpkg)</button>
      <button data-act="export">Export backup</button>
      <button data-act="restore">Restore from backup</button>
      <button data-act="persist">Request persistent storage</button>
      <button data-act="usage">Storage used</button>
    </div>`);

  $('sheet').querySelectorAll('[data-act]').forEach(b => b.onclick = async () => {
    hideSheet();
    const a = b.dataset.act;
    if (a === 'import') { S.expect = 'lesson'; $('file').click(); }
    else if (a === 'export') exportBackup();
    else if (a === 'restore') { S.expect = 'backup'; $('file').click(); }
    else if (a === 'persist') {
      if (navigator.storage && navigator.storage.persist) {
        const ok = await navigator.storage.persist();
        toast(ok ? 'Storage is now persistent' : 'Request declined by the browser');
      } else toast('Not supported here');
    } else if (a === 'usage') {
      const e = await navigator.storage.estimate();
      toast(`${(e.usage / 1e6).toFixed(0)} MB used of ${(e.quota / 1e9).toFixed(1)} GB`);
    }
  });
}

/* ----------------------------------------------------------- listeners */

$('list').addEventListener('click', async e => {
  const del = e.target.closest('[data-del]');
  if (del) {
    const row = S.lessons.find(r => r.man.id === del.dataset.del);
    if (!confirm(`Delete "${row.man.title}"? Your word progress is kept.`)) return;
    await req(tx('lessons', 'readwrite').delete(del.dataset.del));
    await req(tx('media', 'readwrite').delete(del.dataset.del));
    await loadLibrary();
    return;
  }
  const card = e.target.closest('.card');
  if (card) openLesson(card.dataset.id);
});

$('script').addEventListener('click', e => {
  const w = e.target.closest('.w');
  if (w) { showWord(+w.dataset.si, +w.dataset.ti); return; }
  const seg = e.target.closest('.seg');
  if (seg) seek(S.segs[+seg.dataset.si].start);
});

$('file').addEventListener('change', async e => {
  for (const f of e.target.files) {
    try {
      const head = new Uint8Array(await f.slice(0, 4).arrayBuffer());
      const isZip = head[0] === 0x50 && head[1] === 0x4b;   // 'PK'
      if (isZip) await importFile(f);
      else if (f.name.endsWith('.json') || S.expect === 'backup') await importBackup(f);
      else throw new Error('Not a lesson package or backup');
    } catch (err) { toast('Failed: ' + err.message, 4000); console.error(err); }
  }
  e.target.value = '';
});

$('import1').onclick = $('import2').onclick = () => { S.expect = 'lesson'; $('file').click(); };
$('menu').onclick = showMenu;
$('back').onclick = closeLesson;
$('scrim').onclick = hideSheet;

// Cycle the video pane: normal -> large -> hidden. Hidden is useful for
// re-listening to something you have already watched, where the transcript
// is what you want on screen.
$('size').onclick = () => {
  const st = $('stage');
  if (st.classList.contains('audio')) return;
  if (st.classList.contains('big')) { st.classList.remove('big'); st.classList.add('off'); }
  else if (st.classList.contains('off')) st.classList.remove('off');
  else st.classList.add('big');
};

$('prev').onclick = () => goSeg(-1);
$('next').onclick = () => goSeg(1);
$('replay').onclick = () => { if (S.curSeg >= 0) seek(S.segs[S.curSeg].start); };
$('play').onclick = () => { if (S.media) S.media.paused ? S.media.play() : S.media.pause(); };
$('peek').onclick = () => { if (S.curSeg >= 0) $('g' + S.curSeg).classList.toggle('peek'); };
$('tr').onclick = function () {
  document.body.classList.toggle('showtr');
  this.classList.toggle('on', document.body.classList.contains('showtr'));
};

document.addEventListener('keydown', e => {
  if (!S.media) return;
  const k = e.key.toLowerCase();
  if (k === ' ') { e.preventDefault(); $('play').click(); }
  else if (k === 'arrowright') { e.preventDefault(); goSeg(1); }
  else if (k === 'arrowleft') { e.preventDefault(); goSeg(-1); }
  else if (k === 'r') $('replay').click();
  else if (k === 'e') $('peek').click();
  else if (k === 't') $('tr').click();
});

/* -------------------------------------------------------------- start */

(async function boot() {
  db = await openDB();
  await replayEvents();
  await loadLibrary();

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('sw.js').catch(() => {});
  }
  // Ask once. Without this the browser may evict a multi-gigabyte library.
  if (navigator.storage && navigator.storage.persist && !localStorage.getItem('lingua.persist')) {
    localStorage.setItem('lingua.persist', '1');
    navigator.storage.persist().catch(() => {});
  }
})();
