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
const DB_VER = 2;
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
      const d = r.result;
      const names = d.objectStoreNames;
      if (!names.contains('lessons')) d.createObjectStore('lessons', { keyPath: 'id' });
      if (!names.contains('media')) d.createObjectStore('media', { keyPath: 'id' });
      if (!names.contains('events')) {
        d.createObjectStore('events', { keyPath: 'seq', autoIncrement: true })
          .createIndex('ts', 'ts');
      }
      // Generated speech is cached by text+voice: the same phrase is replayed
      // constantly, and a cache hit costs nothing and works offline.
      if (!names.contains('tts')) d.createObjectStore('tts', { keyPath: 'k' });
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
  h += `<div class="row"><button id="explain">Explain in context</button></div>`;
  showSheet(h);
  $('explain').onclick = () => explainWord(si, ti);

  $('sheet').querySelectorAll('[data-set]').forEach(b => b.onclick = async () => {
    await emit('status', { key: S.man.language + ':' + lemma, value: b.dataset.set });
    document.querySelectorAll('.w').forEach(e => {
      const tk = S.segs[+e.dataset.si].tokens[+e.dataset.ti];
      if (tk.lemma === lemma) e.className = e.className.replace(/s-\w+/, 's-' + b.dataset.set);
    });
    updateStat();
    hideSheet();
    nagIfStale();
  });
}

/* -------------------------------------------------------------- backup */

/* Backup pressure.
 *
 * Vocabulary is the only irreplaceable data here - media can be re-imported
 * from the PC, but nothing else holds your word status. It is also tiny, a
 * few KB after years. So the app tracks how much is unsaved and asks.
 *
 * The largest real risk is deleting the app from the home screen, which
 * clears its storage with no warning and no recovery.
 */

const backupState = () => ({
  at: +(localStorage.getItem('lingua.backup.at') || 0),
  count: +(localStorage.getItem('lingua.backup.count') || 0),
});

async function unsavedCount() {
  const total = await req(tx('events').count());
  return Math.max(0, total - backupState().count);
}

async function markBackedUp() {
  localStorage.setItem('lingua.backup.at', Date.now());
  localStorage.setItem('lingua.backup.count', await req(tx('events').count()));
}

async function nagIfStale() {
  const n = await unsavedCount();
  const { at } = backupState();
  const days = at ? (Date.now() - at) / 864e5 : 999;
  if (n >= 25 || (n > 0 && days >= 7)) {
    toast(`${n} changes not backed up — ••• → Export backup`, 5000);
  }
}

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

  // Share sheet on iOS puts it straight into Files or iCloud Drive, which
  // then syncs to the PC on its own.
  if (navigator.canShare && navigator.canShare({ files: [file] })) {
    try {
      await navigator.share({ files: [file], title: name });
      await markBackedUp();
      return;
    } catch (e) { return; }   // cancelled - do not mark as saved
  }
  await markBackedUp();
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

async function showMenu() {
  const lang = S.man ? S.man.language : 'es';
  const known = Object.entries(S.vocab)
    .filter(([k, v]) => k.startsWith(lang + ':') && v === 'known').length;
  const total = Math.round(Object.values(S.studied).reduce((a, b) => a + b, 0) / 60);

  const n = await unsavedCount();
  const { at } = backupState();
  const when = at ? new Date(at).toLocaleDateString() : 'never';
  const warn = n > 0
    ? `<div class="ov">${n} change${n === 1 ? '' : 's'} not backed up · last backup ${when}</div>`
    : `<div class="lm">All changes backed up · ${when}</div>`;

  showSheet(`
    <div class="hw">Lingua</div>
    <div class="lm">${known} words known · ${total} minutes studied</div>
    ${warn}
    <div class="row" style="flex-direction:column">
      <button data-act="practice">Practice / translate</button>
      <button data-act="settings">Settings</button>
      <button data-act="import">Import lesson (.lpkg)</button>
      <button data-act="export">Export backup</button>
      <button data-act="restore">Restore from backup</button>
      <button data-act="persist">Request persistent storage</button>
      <button data-act="usage">Storage used</button>
    </div>`);

  $('sheet').querySelectorAll('[data-act]').forEach(b => b.onclick = async () => {
    hideSheet();
    const a = b.dataset.act;
    if (a === 'practice') showPractice();
    else if (a === 'settings') showSettings();
    else if (a === 'import') { S.expect = 'lesson'; $('file').click(); }
    else if (a === 'export') exportBackup();
    else if (a === 'restore') { S.expect = 'backup'; $('file').click(); }
    else if (a === 'persist') {
      if (navigator.storage && navigator.storage.persist) {
        const ok = await navigator.storage.persist();
        toast(ok ? 'Storage is now persistent' : 'Request declined by the browser');
      } else toast('Not supported here');
    } else if (a === 'usage') showStorage();
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
$('menu').onclick = () => showMenu();
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
$('play').onclick = () => {
  if (!S.media) return;
  if (S.media.paused) { S.media.play(); $('play').textContent = '❚❚'; }
  else { S.media.pause(); $('play').textContent = '▶'; }
};
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
  setTimeout(nagIfStale, 2500);

  if ('serviceWorker' in navigator) {
    navigator.serviceWorker.register('sw.js').catch(() => {});
  }
  // Ask once. Without this the browser may evict a multi-gigabyte library.
  if (navigator.storage && navigator.storage.persist && !localStorage.getItem('lingua.persist')) {
    localStorage.setItem('lingua.persist', '1');
    navigator.storage.persist().catch(() => {});
  }
})();

/* =====================================================================
 * Runtime API features: contextual explanation, chat, and production
 * practice. These are the only parts that need a network connection.
 *
 * The key is kept in localStorage. That is acceptable for a personal app
 * on your own device and would not be for anything shared. Anthropic's
 * API requires an explicit opt-in header for browser calls, below.
 * ===================================================================== */

const API_URL = 'https://api.anthropic.com/v1/messages';
const API_MODEL = 'claude-sonnet-4-5';

const getKey = () => localStorage.getItem('lingua.key') || '';
const setKey = k => localStorage.setItem('lingua.key', k.trim());
const getRegion = () => localStorage.getItem('lingua.region') || 'Mexican';

async function callAPI(system, messages, maxTokens = 1200) {
  const key = getKey();
  if (!key) throw new Error('No API key set. Add one in ••• → Settings.');
  const r = await fetch(API_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-api-key': key,
      'anthropic-version': '2023-06-01',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify({ model: API_MODEL, max_tokens: maxTokens, system, messages }),
  });
  if (!r.ok) {
    const t = await r.text();
    throw new Error(r.status === 401 ? 'API key rejected' : `API error ${r.status}: ${t.slice(0, 120)}`);
  }
  const d = await r.json();
  return d.content.filter(b => b.type === 'text').map(b => b.text).join('\n');
}

function regionLine() {
  // Two different jobs. When the learner PRODUCES Spanish, target their
  // variety. When they are READING someone else's Spanish, explain what that
  // speaker actually meant - normalising a Colombian idiom into Mexican usage
  // would hide the thing they were confused by.
  return `The learner's target variety is ${getRegion()} Spanish, but they listen `
       + `to speakers from across Latin America.\n`
       + `When producing Spanish for them, use ${getRegion()} vocabulary and register.\n`
       + `When explaining Spanish they are reading or hearing, explain it as the `
       + `speaker meant it in their own variety. Name the region when a usage is `
       + `regional, and mention the ${getRegion()} equivalent when it differs.`;
}

/* Window of sentences around the current one. Small on purpose: a few
   hundred tokens rather than a whole transcript. */
function contextWindow(back = 3, fwd = 2) {
  if (S.curSeg < 0) return '';
  const lo = Math.max(0, S.curSeg - back);
  const hi = Math.min(S.segs.length, S.curSeg + fwd + 1);
  return S.segs.slice(lo, hi).map(s =>
    (s.id === S.curSeg ? '>> ' : '   ') + s.text).join('\n');
}

function busy(label) {
  showSheet(`<div class="hw">${esc(label)}</div><div class="lm">Working…</div>`);
}

/* ---- explain a word in its actual context ---- */

async function explainWord(si, ti) {
  const s = S.segs[si], t = s.tokens[ti];
  busy(t.surface);
  try {
    const out = await callAPI(
      `You explain a word to an English speaker reading Spanish.\n\n${regionLine()}\n\n`
      + `Answer in three short parts: what the word means here, what it means `
      + `generally if that differs, and any grammar that makes this sentence work. `
      + `Be brief. Plain text, no markdown.`,
      [{ role: 'user', content:
        `Sentence: ${s.text}\nTranslation: ${s.translation || '(none)'}\n`
        + `Word: ${t.surface}${t.lemma ? ` (lemma ${t.lemma})` : ''}` }],
      700);
    showSheet(`<div class="hw">${esc(t.surface)}</div>`
      + `<div class="gl" style="white-space:pre-wrap">${esc(out)}</div>`
      + `<div class="row"><button id="sheetclose">Close</button></div>`);
    $('sheetclose').onclick = hideSheet;
  } catch (e) { hideSheet(); toast(e.message, 5000); }
}

/* ---- ask about what is happening now ---- */

let chatLog = [];

function showAsk() {
  const ctx = contextWindow();
  showSheet(`
    <div class="hw">Ask</div>
    <div class="lm">${S.curSeg >= 0 ? 'About sentence ' + S.curSeg : 'No sentence playing'}</div>
    ${ctx ? `<div class="nt" style="white-space:pre-wrap">${esc(ctx)}</div>` : ''}
    <textarea id="q" rows="3" placeholder="What does she mean here?"
      style="width:100%;background:#2c2c23;color:var(--fg);border:1px solid var(--line);
             border-radius:9px;padding:10px;font:inherit;font-size:16px"></textarea>
    <div class="row">
      <button id="qsend">Ask</button>
      <button id="qfull">Ask about the whole lesson</button>
    </div>
    <div id="qout" style="white-space:pre-wrap;margin-top:12px"></div>`);
  $('q').focus();
  $('qsend').onclick = () => runAsk(false);
  $('qfull').onclick = () => runAsk(true);
}

async function runAsk(full) {
  const q = $('q').value.trim();
  if (!q) return;
  const out = $('qout');
  out.textContent = 'Thinking…';
  try {
    const ctx = full
      ? S.segs.map(s => `(${s.id}) ${s.text}`).join('\n')
      : contextWindow();
    const text = await callAPI(
      `You help an English speaker studying Spanish through real material.\n\n`
      + `${regionLine()}\n\n`
      + `You may be shown an excerpt of what they are reading. Use it when the `
      + `question refers to it and ignore it otherwise. Answer the question asked, `
      + `briefly. Do not turn every answer into a grammar lesson. Plain text, no markdown.`,
      chatLog.concat([{ role: 'user', content:
        ctx ? `Currently reading:\n${ctx}\n\nQuestion: ${q}` : q }]),
      1200);
    chatLog = chatLog.concat([
      { role: 'user', content: q },
      { role: 'assistant', content: text },
    ]).slice(-8);   // keep the last few turns so cost stays flat
    out.textContent = text;
    $('q').value = '';
  } catch (e) { out.textContent = e.message; }
}

/* ---- say / fix ---- */

function showPractice() {
  showSheet(`
    <div class="hw">Practice</div>
    <div class="lm">Type English to translate, or Spanish to be corrected</div>
    <textarea id="p" rows="3" placeholder="Could I get the check?"
      style="width:100%;background:#2c2c23;color:var(--fg);border:1px solid var(--line);
             border-radius:9px;padding:10px;font:inherit;font-size:16px"></textarea>
    <div class="row">
      <button id="psay">→ Spanish</button>
      <button id="pfix">Check my Spanish</button>
    </div>
    <div id="pout" style="white-space:pre-wrap;margin-top:12px"></div>`);
  $('p').focus();
  $('psay').onclick = () => runPractice('say');
  $('pfix').onclick = () => runPractice('fix');
}

async function runPractice(mode) {
  const text = $('p').value.trim();
  if (!text) return;
  const out = $('pout');
  out.textContent = 'Thinking…';

  // JSON rather than prose, so the Spanish line can be pulled out cleanly
  // and handed to the speech button.
  const say =
    `You turn English into natural Spanish.\n\n${regionLine()}\n\n`
    + `Return JSON only, no fences:\n`
    + `{"spanish":"...","back":"...","note":"..."}\n\n`
    + `spanish: what a native speaker would actually say, not a literal rendering.\n`
    + `back: an English back-translation of your Spanish, so the meaning can be checked.\n`
    + `note: one or two lines on anything worth noticing - a word choice that differs `
    + `from the obvious one, a structure with no English equivalent, a register choice. `
    + `Use an empty string when there is nothing genuinely worth saying.`;
  const fix =
    `You correct a learner's Spanish.\n\n${regionLine()}\n\n`
    + `Return JSON only, no fences:\n`
    + `{"verdict":"...","corrected":"...","why":""}\n\n`
    + `verdict: one short line - correct, understandable but unnatural, or wrong.\n`
    + `corrected: what a native speaker would say. If the original was already fine, `
    + `repeat it unchanged rather than inventing a change.\n`
    + `why: what was off and the rule behind it, naming the error type (gender `
    + `agreement, wrong preposition, ser/estar, false friend). Empty string if correct. `
    + `Do not flag a merely different valid phrasing as an error.`;

  try {
    const raw = await callAPI(mode === 'say' ? say : fix,
      [{ role: 'user', content: text }], 900);
    let d;
    try {
      d = JSON.parse(raw.replace(/^```(?:json)?\s*|\s*```$/g, '').trim());
    } catch (e) { out.textContent = raw; return; }

    const spanish = mode === 'say' ? d.spanish : d.corrected;
    let h = '';
    if (mode === 'fix' && d.verdict) h += `<div class="lm">${esc(d.verdict)}</div>`;
    h += `<div class="gl" style="display:flex;align-items:center;flex-wrap:wrap">`
       + `<span>${esc(spanish || '')}</span>${spanish ? speakBtn(spanish) : ''}</div>`;
    if (mode === 'say' && d.back) h += `<div class="lm">${esc(d.back)}</div>`;
    const note = mode === 'say' ? d.note : d.why;
    if (note) h += `<div class="nt" style="white-space:pre-wrap">${esc(note)}</div>`;
    out.innerHTML = h;
  } catch (e) { out.textContent = e.message; }
}

/* ---- storage report ---- */
/* Safari fuzzes or zeroes navigator.storage.estimate().usage, since exact
   figures are a fingerprinting vector. Summing the stored blobs gives the
   real answer, so we show both and let the measured number lead. */

async function showStorage() {
  busy('Storage');
  const media = await req(tx('media').getAll());
  const bytes = media.reduce((a, m) => a + (m.blob ? m.blob.size : 0), 0);

  let quota = 0, reported = 0, persisted = false;
  if (navigator.storage) {
    if (navigator.storage.estimate) {
      const e = await navigator.storage.estimate();
      quota = e.quota || 0; reported = e.usage || 0;
    }
    if (navigator.storage.persisted) {
      persisted = await navigator.storage.persisted().catch(() => false);
    }
  }

  const rows = media.length
    ? (await req(tx('lessons').getAll())).map(r => {
        const m = media.find(x => x.id === r.man.id);
        return `<div class="lm">${esc(r.man.title)} — `
             + `${m ? (m.blob.size / 1e6).toFixed(1) : '?'} MB</div>`;
      }).join('')
    : '<div class="lm">No lessons stored</div>';

  showSheet(`
    <div class="hw">${(bytes / 1e6).toFixed(0)} MB stored</div>
    <div class="lm">${media.length} lesson${media.length === 1 ? '' : 's'} ·
      ${quota ? (quota / 1e9).toFixed(1) + ' GB available' : 'quota unknown'}</div>
    <div class="nt">Eviction protection: ${persisted ? 'on' : 'OFF — data may be cleared'}
      ${reported === 0 && bytes > 0 ? '<br>Safari reports 0 MB used; that is a known quirk, not a problem.' : ''}
    </div>
    ${rows}
    <div class="row"><button id="sheetclose">Close</button></div>`);
  $('sheetclose').onclick = hideSheet;
}

/* ---- settings ---- */

function showSettings() {
  showSheet(`
    <div class="hw">Settings</div>
    <div class="lm">The key is stored on this device only</div>
    <input id="k" type="password" placeholder="sk-ant-..." value="${esc(getKey())}"
      style="width:100%;background:#2c2c23;color:var(--fg);border:1px solid var(--line);
             border-radius:9px;padding:11px;font:inherit;font-size:16px;margin-bottom:8px">
    <input id="rg" placeholder="Mexican" value="${esc(getRegion())}"
      style="width:100%;background:#2c2c23;color:var(--fg);border:1px solid var(--line);
             border-radius:9px;padding:11px;font:inherit;font-size:16px">
    <div class="lm" style="margin-top:6px">Variety used when writing Spanish for you.
      Material you read is still explained in the speaker's own variety.</div>

    <div class="hw" style="margin-top:18px;font-size:17px">Speech</div>
    <input id="tk" type="password" placeholder="ElevenLabs key" value="${esc(getTtsKey())}"
      style="width:100%;background:#2c2c23;color:var(--fg);border:1px solid var(--line);
             border-radius:9px;padding:11px;font:inherit;font-size:16px;margin-bottom:8px">
    <div class="lm">Voice: <span id="vname">${esc(getVoiceName())}</span></div>
    <div id="vlist"></div>
    <div class="row">
      <button id="vload">Load voices</button>
      <button id="ksave">Save</button>
    </div>`);

  $('vload').onclick = async () => {
    localStorage.setItem('lingua.tts.key', $('tk').value.trim());
    const box = $('vlist');
    box.innerHTML = '<div class="lm">Loading…</div>';
    try {
      const vs = await loadVoices();
      const free = vs.filter(v => v.free), paid = vs.filter(v => !v.free);
      const opt = v => `<option value="${esc(v.id)}"`
        + `${v.id === getVoice() ? ' selected' : ''}>${esc(v.name)}</option>`;

      box.innerHTML = `<select id="vsel" style="width:100%;background:#2c2c23;
        color:var(--fg);border:1px solid var(--line);border-radius:9px;
        padding:11px;font:inherit;font-size:16px;margin:6px 0">`
        + (free.length ? `<optgroup label="Works on any plan (${free.length})">`
                       + free.map(opt).join('') + `</optgroup>` : '')
        + (paid.length ? `<optgroup label="Voice Library — needs a paid plan (${paid.length})">`
                       + paid.map(opt).join('') + `</optgroup>` : '')
        + `</select>`
        + `<div class="row"><button id="vtest">Test this voice</button></div>`
        + `<div class="lm">Library voices play on the ElevenLabs website but are `
        + `blocked through the API on the free plan.</div>`;

      $('vsel').onchange = () => {
        const o = $('vsel').selectedOptions[0];
        localStorage.setItem('lingua.tts.voice', o.value);
        localStorage.setItem('lingua.tts.name', o.textContent);
        $('vname').textContent = o.textContent;
      };
      $('vtest').onclick = e =>
        speak('Hoy vamos a pedir algo de comer. ¿Qué se te antoja?', e.target);
      if (!getVoice() && vs.length) $('vsel').onchange();
    } catch (e) { box.innerHTML = `<div class="ov">${esc(e.message)}</div>`; }
  };

  $('ksave').onclick = () => {
    setKey($('k').value);
    localStorage.setItem('lingua.region', $('rg').value.trim() || 'Mexican');
    localStorage.setItem('lingua.tts.key', $('tk').value.trim());
    hideSheet(); toast('Saved');
  };
}

$('ask').onclick = showAsk;

/* =====================================================================
 * Speech playback via ElevenLabs.
 *
 * Audio is cached in IndexedDB keyed by text + voice, so replaying a
 * phrase you have heard before is instant, free, and works offline.
 * ===================================================================== */

const TTS_URL = 'https://api.elevenlabs.io/v1/text-to-speech/';
const TTS_MODEL = 'eleven_multilingual_v2';

const getTtsKey = () => localStorage.getItem('lingua.tts.key') || '';
const getVoice = () => localStorage.getItem('lingua.tts.voice') || '';
const getVoiceName = () => localStorage.getItem('lingua.tts.name') || 'none selected';

async function hashKey(text, voice) {
  const buf = new TextEncoder().encode(voice + '|' + text);
  const d = await crypto.subtle.digest('SHA-256', buf);
  return [...new Uint8Array(d)].slice(0, 12)
    .map(b => b.toString(16).padStart(2, '0')).join('');
}

async function speak(text, btn) {
  const key = getTtsKey(), voice = getVoice();
  if (!key || !voice) { toast('Set an ElevenLabs key and voice in Settings', 4000); return; }

  const k = await hashKey(text, voice);
  let hit = null;
  try { hit = await req(tx('tts').get(k)); } catch (e) { /* store may be new */ }

  if (!hit) {
    if (btn) btn.textContent = '…';
    try {
      const r = await fetch(TTS_URL + voice, {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'xi-api-key': key },
        body: JSON.stringify({
          text,
          model_id: TTS_MODEL,
          voice_settings: { stability: 0.5, similarity_boost: 0.75 },
        }),
      });
      if (!r.ok) {
        let msg = `TTS error ${r.status}`;
        if (r.status === 401) msg = 'ElevenLabs key rejected';
        else if (r.status === 402) {
          // Free accounts cannot use community Voice Library voices through
          // the API, even though the same voice works on the ElevenLabs site.
          const body = await r.text().catch(() => '');
          msg = /library voices/i.test(body)
            ? 'Free plan cannot use Voice Library voices via API. Pick a premade voice, or upgrade.'
            : 'ElevenLabs quota exhausted for this month.';
        }
        throw new Error(msg);
      }
      const blob = await r.blob();
      await req(tx('tts', 'readwrite').put({ k, blob }));
      hit = { k, blob };
    } catch (e) {
      if (btn) btn.textContent = '▶';
      toast(e.message, 4000);
      return;
    }
  }

  if (btn) btn.textContent = '▶';
  const url = URL.createObjectURL(hit.blob);
  const a = new Audio(url);
  a.onended = () => URL.revokeObjectURL(url);
  a.play().catch(() => toast('Could not play audio'));
}

/* Delegated so buttons rendered into the sheet after the fact still work. */
document.addEventListener('click', e => {
  const b = e.target.closest('[data-speak]');
  if (b) speak(b.dataset.speak, b);
});

function speakBtn(text) {
  return `<button data-speak="${esc(text)}"
    style="border:1px solid var(--line);background:#2c2c23;border-radius:8px;
           padding:6px 13px;margin-left:8px;font-size:15px">▶</button>`;
}

/* ---- voice picker ---- */

async function loadVoices() {
  const key = getTtsKey();
  if (!key) throw new Error('Enter your ElevenLabs key first');
  const r = await fetch('https://api.elevenlabs.io/v2/voices?page_size=100',
                        { headers: { 'xi-api-key': key } });
  if (!r.ok) throw new Error(r.status === 401 ? 'Key rejected' : `Error ${r.status}`);
  const d = await r.json();
  const list = (d.voices || []).map(v => ({
    id: v.voice_id,
    name: v.name,
    // 'premade' voices work on every plan; library voices need a paid one.
    free: v.category === 'premade' || v.category === 'generated' || v.category === 'cloned',
    labels: v.labels || {},
  }));
  // Usable voices first, so a free account sees what it can actually play.
  list.sort((a, b) => (b.free - a.free) || a.name.localeCompare(b.name));
  return list;
}
