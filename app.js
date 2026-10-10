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
const DB_VER = 3;
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
      // Saved flashcards, exported to Anki rather than reviewed here.
      if (!names.contains('cards')) {
        d.createObjectStore('cards', { keyPath: 'id', autoIncrement: true });
      }
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
  days: {},         // 'YYYY-MM-DD' -> { read, listen } seconds
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

/* Local-day key. Deliberately local rather than UTC: a session at 11pm
   belongs to that evening, not to the next day. */
function dayKey(ts) {
  const d = new Date(ts);
  return d.getFullYear() + '-'
       + String(d.getMonth() + 1).padStart(2, '0') + '-'
       + String(d.getDate()).padStart(2, '0');
}

function hm(secs) {
  const m = Math.round(secs / 60);
  if (m < 60) return m + 'm';
  return Math.floor(m / 60) + 'h ' + String(m % 60).padStart(2, '0') + 'm';
}

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
  else if (ev.kind === 'study') {
    S.studied[ev.lesson] = (S.studied[ev.lesson] || 0) + ev.value;
    // Bucket by local day. Events have always carried a timestamp, so this
    // works retroactively over everything already logged.
    const day = dayKey(ev.ts);
    const d = S.days[day] || (S.days[day] = { read: 0, listen: 0 });
    d[ev.mode === 'listen' ? 'listen' : 'read'] += ev.value;
  }
}

async function replayEvents() {
  S.vocab = {}; S.studied = {}; S.days = {};
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
  stage.innerHTML = `<${tag} id="v" controls playsinline preload="metadata"></${tag}>`
    + `<div id="cap"></div>`;
  S.media = $('v');
  S.media.src = URL.createObjectURL(m.blob);

  buildFlat();
  renderScript();

  $('title').textContent = row.man.title;
  $('library').classList.add('hidden');
  $('player').classList.remove('hidden');
  $('back').classList.remove('hidden');
  $('size').classList.toggle('hidden', row.man.kind === 'audio');
  $('cc').classList.toggle('hidden', row.man.kind === 'audio');
  $('fs').classList.toggle('hidden', row.man.kind === 'audio');
  applyCapMode(capMode(), false);
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
  setImmersive(false);
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

/* One sentence, drawn over the video. Shares the token markup with the
   transcript so highlighting and word status stay in step automatically. */
/* Scroll the transcript pane and nothing else.
 *
 * scrollIntoView on iOS does not confine itself to the intended container -
 * it scrolls every scrollable ancestor including the document. With the body
 * fixed that manifested as the whole page creeping upward, taking the top bar
 * out of reach and pushing the control bar off the bottom. Setting scrollTop
 * on the pane directly cannot do that.
 */
function centreInScript(el) {
  const box = $('script');
  if (!box || !el) return;

  // Measured from the pane, not from offsetTop. offsetTop is relative to the
  // nearest POSITIONED ancestor, and since body is fixed that was the body -
  // so the value included the top bar and the whole video, and the scroll
  // overshot by exactly that much, hiding the current line under the video.
  const br = box.getBoundingClientRect();
  const er = el.getBoundingClientRect();
  const delta = (er.top - br.top) - (br.height - er.height) / 2;

  const max = Math.max(0, box.scrollHeight - box.clientHeight);
  const want = Math.max(0, Math.min(max, box.scrollTop + delta));
  if (Math.abs(box.scrollTop - want) < 12) return;
  try { box.scrollTo({ top: want, behavior: 'smooth' }); }
  catch (e) { box.scrollTop = want; }
}

function renderCaption(si) {
  const cap = $('cap');
  if (!cap) return;
  if (si < 0) { cap.innerHTML = ''; return; }
  const s = S.segs[si];
  let out = '', cur = 0;
  s.tokens.forEach((t, ti) => {
    if (t.start_char > cur) out += esc(s.text.slice(cur, t.start_char));
    out += `<span class="w s-${statusOf(t.lemma)}" data-si="${si}" data-ti="${ti}">`
         + `${esc(s.text.slice(t.start_char, t.end_char))}</span>`;
    cur = t.end_char;
  });
  if (cur < s.text.length) out += esc(s.text.slice(cur));
  cap.classList.remove('peek');
  cap.innerHTML = `<div class="es">${out}</div>`
    + `<div class="tr">${esc(s.translation || '')}</div>`;
}

function updateStat() {
  if (!S.doc) return;
  const { total, known } = lessonKnown(S.doc, S.man.language);
  $('stat').textContent = total ? Math.round(known / total * 100) + '%' : '';
}

/* --------------------------------------------------------- play clock */
/* Study time is accumulated in short bursts rather than one event per
   session, so a crash or a swipe-away loses at most a few seconds. */

/* Study time is credited from the WALL CLOCK between ticks, not as a flat
 * amount per tick.
 *
 * iOS throttles timers when the screen locks or the app backgrounds, so a
 * fixed 15-per-tick both under-counts a late tick and loses whatever played
 * between the last tick and a pause. Measuring the delta and flushing on
 * pause fixes both. The delta is capped so a long throttle gap - or a device
 * that slept with the media element technically unpaused - cannot credit
 * time nobody spent.
 */
const TICK_MS = 15000;
const MAX_CREDIT = 90;        // seconds; a gap longer than this is not study

function studyMode() {
  // Audio-only lessons, and video deliberately hidden, are listening
  // practice; reading along with the text is a different skill and is worth
  // counting separately.
  if (!S.man) return 'read';
  if (S.man.kind === 'audio') return 'listen';
  const st = $('stage');
  return (st && st.classList.contains('off')) ? 'listen' : 'read';
}

function creditTime() {
  if (!S.media || !S.man) return;
  const now = Date.now();
  if (!S._last) { S._last = now; return; }
  const secs = Math.round((now - S._last) / 1000);
  S._last = now;
  if (secs <= 0) return;
  emit('study', {
    lesson: S.man.id,
    value: Math.min(secs, MAX_CREDIT),
    mode: studyMode(),
  });
}

function startClock() {
  if (S._clock) return;
  S._last = Date.now();
  S._clock = setInterval(() => {
    if (S.media && !S.media.paused) creditTime();
    else { stopClock(); }
  }, TICK_MS);
}

function stopClock() {
  if (S._clock) {
    clearInterval(S._clock);
    S._clock = null;
    creditTime();              // do not lose the partial interval
  }
  S._last = null;
}

function loop() {
  S.tick = requestAnimationFrame(loop);
  if (!S.media) return;
  const t = S.media.currentTime;

  const si = segAt(t);

  if (si !== S.curSeg) {
    document.querySelectorAll('.seg.cur').forEach(e => e.classList.remove('cur'));
    document.querySelectorAll('.seg.peek').forEach(e => e.classList.remove('peek'));
    if (si >= 0) {
      const el = $('g' + si);
      el.classList.add('cur');
      centreInScript(el);
    }
    renderCaption(si);
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
      document.querySelectorAll(`.w[data-si="${f.si}"][data-ti="${f.ti}"]`)
        .forEach(el => el.classList.add('on'));
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
/* Navigation works off the playhead, never off S.curSeg.
 *
 * curSeg is the sentence CONTAINING the playhead, and it is -1 whenever
 * playback sits in a gap between sentences - which in real material is a
 * lot of the time. Treating -1 as "not started" made prev/next jump to the
 * beginning of the lesson, and made peek do nothing.
 */

// The sentence containing t, or -1.
function segAt(t) {
  for (let i = 0; i < S.segs.length; i++) {
    const s = S.segs[i];
    if (s.start != null && t >= s.start - 0.05 && t <= s.end + 0.05) return i;
  }
  return -1;
}

// The sentence containing t, or failing that the last one that has started.
// In a gap this is the sentence you just heard, which is what you mean when
// you reach for a control.
function segNear(t) {
  let best = -1;
  for (let i = 0; i < S.segs.length; i++) {
    const s = S.segs[i];
    if (s.start == null) continue;
    if (s.start <= t + 0.05) best = i; else break;
  }
  return best;
}

function goSeg(d) {
  if (!S.segs.length) return;
  const t = S.media ? S.media.currentTime : 0;

  if (d > 0) {
    const nxt = S.segs.find(s => s.start != null && s.start > t + 0.15);
    seek(nxt ? nxt.start : S.segs[S.segs.length - 1].start);
    return;
  }

  const inside = segAt(t);
  // Inside a sentence, "back" means the one before it. In a gap, it means
  // the sentence that just finished.
  let target = inside >= 0 ? inside - 1 : segNear(t);
  if (target < 0) target = 0;
  seek(S.segs[target].start);
}

/* ---------------------------------------------------------- audio slice */
/* Play a span of the lesson audio and stop. Uses the word timestamps that
   already exist, so hearing a word costs nothing and works offline - and you
   hear the real speaker rather than a synthetic approximation.

   The playhead is restored afterwards so looking a word up never loses your
   place. */

let sliceWatch = null;

function playSlice(start, end, btn) {
  if (!S.media || start == null) { toast('No timing for that word'); return; }
  const m = S.media;
  const restore = m.currentTime;
  const wasPlaying = !m.paused;

  const pad = 0.08;
  let a = Math.max(0, start - pad);
  let b = end + pad * 1.6;
  if (b - a < 0.28) b = a + 0.28;      // very short words need a moment to register

  if (sliceWatch) { cancelAnimationFrame(sliceWatch); sliceWatch = null; }
  if (btn) btn.textContent = '‖';

  const done = () => {
    if (sliceWatch) { cancelAnimationFrame(sliceWatch); sliceWatch = null; }
    m.pause();
    m.currentTime = restore;
    if (btn) btn.textContent = '▶';
    if (wasPlaying) m.play().catch(() => {});
  };

  const watch = () => {
    if (m.currentTime >= b) { done(); return; }
    sliceWatch = requestAnimationFrame(watch);
  };

  m.currentTime = a;
  m.play().then(() => { sliceWatch = requestAnimationFrame(watch); })
          .catch(() => done());
}

/* --------------------------------------------------------------- sheet */

/* Keep the sheet above the on-screen keyboard.
 *
 * iOS does not shrink the layout viewport when the keyboard opens - it draws
 * the keyboard over the page, so a bottom-anchored fixed element ends up
 * underneath it with no event to react to. visualViewport does report the
 * change, and the difference between it and window.innerHeight is the
 * keyboard's height.
 */
function fitSheet() {
  const vv = window.visualViewport;
  const sheet = $('sheet');
  if (!sheet || !vv) return;
  const kb = Math.max(0, window.innerHeight - vv.height - vv.offsetTop);
  S.keyboard = kb > 60;
  sheet.style.bottom = kb + 'px';
  // Leave room for the sheet itself to scroll when the keyboard is large.
  sheet.style.maxHeight = Math.max(200, vv.height - 24) + 'px';
}

if (window.visualViewport) {
  window.visualViewport.addEventListener('resize', fitSheet);
  window.visualViewport.addEventListener('scroll', fitSheet);
}

// Bring a focused field into view without scrollIntoView, which on iOS
// scrolls every ancestor including the document.
function revealInSheet(el) {
  const sheet = $('sheet');
  if (!sheet || !el) return;
  const want = Math.max(0, el.offsetTop - 12);
  if (sheet.scrollHeight > sheet.clientHeight) sheet.scrollTop = want;
}

function showSheet(html) {
  const sheet = $('sheet');
  sheet.innerHTML = html;
  sheet.classList.add('up');
  $('scrim').classList.add('up');
  fitSheet();
}
function hideSheet() {
  if (sliceWatch) { cancelAnimationFrame(sliceWatch); sliceWatch = null; }
  const sheet = $('sheet');
  document.activeElement && document.activeElement.blur();
  sheet.classList.remove('up');
  $('scrim').classList.remove('up');
  sheet.style.bottom = '';
  sheet.style.maxHeight = '';
  S.keyboard = false;
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
  const hasTime = t.start != null;
  h += `<div class="row">`
     + (hasTime ? `<button id="hearw">▶ word</button>` : '')
     + (s.start != null ? `<button id="hears">▶ sentence</button>` : '')
     + `</div>`
     + (t.interpolated ? `<div class="lm">timing for this word is estimated</div>` : '')
     + `<div class="row"><button id="explain">Explain in context</button></div>`
     + `<div class="lm" style="margin-top:10px">Capture this sentence</div>`
     + `<div class="row">`
     + `<button id="cps">Copy ES</button>`
     + (s.translation ? `<button id="cpt">Copy EN</button>` : '')
     + `<button id="dls">Save audio</button>`
     + `</div>`;
  showSheet(h);

  if (hasTime) $('hearw').onclick = e => playSlice(t.start, t.end, e.currentTarget);
  if (s.start != null) $('hears').onclick = e => playSlice(s.start, s.end, e.currentTarget);
  $('explain').onclick = () => explainWord(si, ti);
  $('cps').onclick = e => copyText(s.text, e.currentTarget);
  if (s.translation) $('cpt').onclick = e => copyText(s.translation, e.currentTarget);
  // The lesson's own audio is inside the mp4 and cannot be handed over as a
  // file from here, so the saved clip is spoken rather than lifted.
  $('dls').onclick = e => saveAudioFile(s.text, e.currentTarget);

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
  localStorage.setItem('lingua.nag.at', Date.now());
}

const NAG_DAYS = { never: 0, weekly: 7, monthly: 30, quarterly: 90 };
const getNagEvery = () => localStorage.getItem('lingua.nag') || 'monthly';

/* Launch only, and at most once per chosen interval.
 *
 * The earlier version compared "days since last backup" against a threshold,
 * which for someone who had never backed up was effectively infinite - so it
 * fired on every single change. Rate-limiting on when we last NAGGED, rather
 * than when the user last backed up, is what actually bounds the frequency.
 */
async function nagIfStale() {
  const every = NAG_DAYS[getNagEvery()];
  if (!every) return;

  const last = +(localStorage.getItem('lingua.nag.at') || 0);
  if (last && (Date.now() - last) / 864e5 < every) return;

  const n = await unsavedCount();
  if (n === 0) return;

  const { at } = backupState();
  const since = at ? Math.round((Date.now() - at) / 864e5) : null;
  localStorage.setItem('lingua.nag.at', Date.now());
  toast(since === null
    ? `${n} changes have never been backed up — ••• → Export backup`
    : `${n} changes since your last backup ${since} days ago`, 6000);
}

async function exportBackup() {
  const events = await req(tx('events').getAll());
  const lessons = (await req(tx('lessons').getAll())).map(r => ({
    id: r.man.id, title: r.man.title, language: r.man.language,
  }));
  const cards = await req(tx('cards').getAll()).catch(() => []);
  const blob = new Blob([JSON.stringify({
    format: 2, exported: new Date().toISOString(), device: DEVICE,
    lessons, events, cards,
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
  let addedCards = 0;
  if (Array.isArray(data.cards) && data.cards.length) {
    const have = new Set((await req(tx('cards').getAll()).catch(() => []))
      .map(c => c.created + '|' + c.back));
    const cs = tx('cards', 'readwrite');
    for (const c of data.cards) {
      if (have.has(c.created + '|' + c.back)) continue;
      const { id, ...rest } = c;
      cs.add(rest); addedCards++;
    }
  }
  await replayEvents();
  renderLibrary(); updateStat();
  toast(`Merged ${added} events` + (addedCards ? `, ${addedCards} cards` : ''));
}

/* ---------------------------------------------------------------- menu */

async function showMenu() {
  const lang = S.man ? S.man.language : 'es';
  const known = Object.entries(S.vocab)
    .filter(([k, v]) => k.startsWith(lang + ':') && v === 'known').length;
  const t = totals();

  const n = await unsavedCount();
  S.cardCount = await cardCount();
  const todaySecs = dayTotal(dayKey(Date.now()));
  const todayStr = todaySecs >= 30 ? hm(todaySecs) : '';
  const { at } = backupState();
  const when = at ? new Date(at).toLocaleDateString() : 'never';
  const warn = n > 0
    ? `<div class="ov">${n} change${n === 1 ? '' : 's'} not backed up · last backup ${when}</div>`
    : `<div class="lm">All changes backed up · ${when}</div>`;

  showSheet(`
    <div class="hw">Lingua</div>
    <div class="lm">${known} words known · ${hm(t.all)} studied · ${streak()}-day streak</div>
    ${warn}
    <div class="row" style="flex-direction:column">
      <button data-act="log">Study log${todayStr ? ' · ' + todayStr : ''}</button>
      <button data-act="practice">Practice / translate</button>
      <button data-act="cards">Flashcards${S.cardCount ? ' (' + S.cardCount + ')' : ''}</button>
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
    if (a === 'log') showLog();
    else if (a === 'practice') showPractice();
    else if (a === 'cards') showCards();
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

$('stage').addEventListener('click', e => {
  const w = e.target.closest('#cap .w');
  if (!w) return;
  e.stopPropagation();
  if (S.media && !S.media.paused) { S.media.pause(); $('play').textContent = '▶'; }
  showWord(+w.dataset.si, +w.dataset.ti);
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
// normal -> big -> cinema -> hidden -> normal
$('size').onclick = () => {
  const st = $('stage');
  if (st.classList.contains('audio')) return;
  const c = st.classList;
  if (c.contains('big')) { c.remove('big'); c.add('cinema'); }
  else if (c.contains('cinema')) { c.remove('cinema'); c.add('off'); }
  else if (c.contains('off')) c.remove('off');
  else c.add('big');
};

$('prev').onclick = () => goSeg(-1);
$('next').onclick = () => goSeg(1);
$('replay').onclick = () => {
  const i = S.media ? segNear(S.media.currentTime) : -1;
  if (i >= 0) seek(S.segs[i].start);
};
$('play').onclick = () => {
  if (!S.media) return;
  if (S.media.paused) { S.media.play(); $('play').textContent = '❚❚'; }
  else { S.media.pause(); $('play').textContent = '▶'; }
};
$('peek').onclick = () => {
  const i = S.curSeg >= 0
    ? S.curSeg
    : (S.media ? segNear(S.media.currentTime) : -1);
  if (i < 0) return;
  const el = $('g' + i);
  if (el) el.classList.toggle('peek');
  const cap = $('cap');
  if (cap) cap.classList.toggle('peek');
};
$('tr').onclick = function () {
  document.body.classList.toggle('showtr');
  this.classList.toggle('on', document.body.classList.contains('showtr'));
};

document.addEventListener('keydown', e => {
  if (e.key === 'Escape' && document.body.classList.contains('immersive')) {
    setImmersive(false);
    return;
  }
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
  // The body is fixed, so any document scroll is something misbehaving.
  // Snap it back rather than leaving controls out of reach.
  const pin = () => {
    if (S.keyboard) return;          // the keyboard legitimately shifts things
    if (window.scrollY || window.scrollX) window.scrollTo(0, 0);
  };
  window.addEventListener('scroll', pin, { passive: true });
  window.addEventListener('resize', pin);
  window.addEventListener('orientationchange', () => setTimeout(pin, 250));

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
  const q = $('q');
  q.addEventListener('focus', () => setTimeout(() => { fitSheet(); revealInSheet(q); }, 320));
  q.focus();
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
  const p = $('p');
  p.addEventListener('focus', () => setTimeout(() => { fitSheet(); revealInSheet(p); }, 320));
  p.focus();
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
    const front = mode === 'say' ? text : (d.back || text);
    if (spanish) {
      h += `<div class="row">`
         + `<button id="cpes">Copy ES</button>`
         + `<button id="cpen">Copy EN</button>`
         + `<button id="dlaudio">Save audio</button>`
         + `</div>`
         + `<div class="row"><button id="savecard">Add to card batch</button></div>`;
    }
    out.innerHTML = h;

    if (spanish) {
      $('cpes').onclick = e => copyText(spanish, e.currentTarget);
      $('cpen').onclick = e => copyText(front, e.currentTarget);
      $('dlaudio').onclick = e => saveAudioFile(spanish, e.currentTarget);
      $('savecard').onclick = async () => {
        // Generate the audio now if it isn't cached, so the card leaves here
        // complete rather than silent.
        if (getTtsKey() && getVoice()) {
          const k = await hashKey(spanish, getVoice());
          const have = await req(tx('tts').get(k)).catch(() => null);
          if (!have) await speak(spanish, null);
        }
        await saveCard(front, spanish, note, getVoice());
        $('savecard').textContent = 'Added ✓';
        $('savecard').disabled = true;
      };
    }
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
    </div>

    <div class="hw" style="margin-top:18px;font-size:17px">Backup reminder</div>
    <select id="ng" style="width:100%;background:#2c2c23;color:var(--fg);
      border:1px solid var(--line);border-radius:9px;padding:11px;
      font:inherit;font-size:16px">
      ${['never', 'weekly', 'monthly', 'quarterly'].map(k =>
        `<option value="${k}"${getNagEvery() === k ? ' selected' : ''}>${k}</option>`).join('')}
    </select>
    <div class="lm" style="margin-top:6px">Shown at most once per period, on launch only.
      Backup status is always visible at the top of this menu.</div>

    <div class="row"><button id="ksave">Save</button></div>`);

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
    localStorage.setItem('lingua.nag', $('ng').value);
    hideSheet(); toast('Saved');
  };
}

$('ask').onclick = showAsk;

/* Caption language is its own setting, separate from the transcript's EN
   toggle: watching and reading along want different things. */
const CAP_MODES = [
  { id: 'off',  label: 'CC',    say: 'Captions off' },
  { id: 'es',   label: 'ES',    say: 'Spanish captions' },
  { id: 'en',   label: 'EN',    say: 'English captions' },
  { id: 'both', label: 'ES·EN', say: 'Spanish and English captions' },
];

function capMode() {
  const want = localStorage.getItem('lingua.cap');
  return CAP_MODES.find(m => m.id === want) ? want : 'es';
}

function applyCapMode(mode, announce) {
  localStorage.setItem('lingua.cap', mode);
  const m = CAP_MODES.find(x => x.id === mode) || CAP_MODES[1];
  const cap = $('cap');
  if (cap) {
    CAP_MODES.forEach(x => cap.classList.remove('m-' + x.id));
    cap.classList.add('m-' + m.id);
  }
  const btn = $('cc');
  if (btn) {
    btn.textContent = m.label;
    btn.classList.toggle('on', m.id !== 'off');
    btn.title = m.say;
  }
  if (announce) toast(m.say, 1400);
}

/* Immersive mode: our own full-screen layout, not the browser's.
 *
 * A real requestFullscreen on iOS hands the video to the native player,
 * which paints over the DOM and takes the tappable captions with it. Sizing
 * the stage to the viewport ourselves keeps them.
 */
function setImmersive(on) {
  document.body.classList.toggle('immersive', on);
  $('exitfs').style.display = on ? 'block' : 'none';
  $('fs').classList.toggle('on', on);
  // Captions are the whole point of this mode, so turn them on if they are off.
  if (on && capMode() === 'off') applyCapMode('es', true);
}

$('fs').onclick = () => setImmersive(!document.body.classList.contains('immersive'));
$('exitfs').onclick = () => setImmersive(false);

$('cc').onclick = () => {
  const i = CAP_MODES.findIndex(m => m.id === capMode());
  applyCapMode(CAP_MODES[(i + 1) % CAP_MODES.length].id, true);
};

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

/* =====================================================================
 * Flashcard export.
 *
 * Cards are not reviewed here - Anki is better at scheduling than anything
 * worth building. The app only collects them and hands them over, with the
 * audio already generated so a card arrives complete.
 *
 * The zip is written with everything STORED rather than deflated. mp3 does
 * not compress and the manifest is tiny, so it costs nothing and avoids
 * needing a compressor.
 * ===================================================================== */

const CRC_TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    t[i] = c >>> 0;
  }
  return t;
})();

function crc32(bytes) {
  let c = 0xFFFFFFFF;
  for (let i = 0; i < bytes.length; i++) {
    c = CRC_TABLE[(c ^ bytes[i]) & 0xFF] ^ (c >>> 8);
  }
  return (c ^ 0xFFFFFFFF) >>> 0;
}

function zipStore(entries) {
  // entries: [{ name, bytes }]
  const enc = new TextEncoder();
  const parts = [];
  const central = [];
  let offset = 0;

  for (const e of entries) {
    const nameBytes = enc.encode(e.name);
    const crc = crc32(e.bytes);
    const size = e.bytes.length;

    const lh = new DataView(new ArrayBuffer(30));
    lh.setUint32(0, 0x04034b50, true);
    lh.setUint16(4, 20, true);          // version needed
    lh.setUint16(6, 0, true);           // flags
    lh.setUint16(8, 0, true);           // method: stored
    lh.setUint16(10, 0, true);          // time
    lh.setUint16(12, 0, true);          // date
    lh.setUint32(14, crc, true);
    lh.setUint32(18, size, true);
    lh.setUint32(22, size, true);
    lh.setUint16(26, nameBytes.length, true);
    lh.setUint16(28, 0, true);          // extra

    parts.push(new Uint8Array(lh.buffer), nameBytes, e.bytes);

    const cd = new DataView(new ArrayBuffer(46));
    cd.setUint32(0, 0x02014b50, true);
    cd.setUint16(4, 20, true);
    cd.setUint16(6, 20, true);
    cd.setUint16(8, 0, true);
    cd.setUint16(10, 0, true);
    cd.setUint16(12, 0, true);
    cd.setUint16(14, 0, true);
    cd.setUint32(16, crc, true);
    cd.setUint32(20, size, true);
    cd.setUint32(24, size, true);
    cd.setUint16(28, nameBytes.length, true);
    cd.setUint16(30, 0, true);
    cd.setUint16(32, 0, true);
    cd.setUint16(34, 0, true);
    cd.setUint16(36, 0, true);
    cd.setUint32(38, 0, true);
    cd.setUint32(42, offset, true);
    central.push(new Uint8Array(cd.buffer), nameBytes);

    offset += 30 + nameBytes.length + size;
  }

  const cdSize = central.reduce((a, b) => a + b.length, 0);
  const eocd = new DataView(new ArrayBuffer(22));
  eocd.setUint32(0, 0x06054b50, true);
  eocd.setUint16(8, entries.length, true);
  eocd.setUint16(10, entries.length, true);
  eocd.setUint32(12, cdSize, true);
  eocd.setUint32(16, offset, true);

  return new Blob([...parts, ...central, new Uint8Array(eocd.buffer)],
                  { type: 'application/zip' });
}

/* ---- saving ---- */

async function saveCard(front, back, note, voice) {
  const audioKey = back && voice ? await hashKey(back, voice) : null;
  await req(tx('cards', 'readwrite').add({
    front, back, note: note || '',
    audio: audioKey,
    lang: S.man ? S.man.language : 'es',
    source: S.man ? S.man.title : null,
    created: Date.now(),
  }));
  toast('Card saved');
}

async function cardCount() {
  try { return await req(tx('cards').count()); } catch (e) { return 0; }
}

/* ---- export ---- */

async function exportCards() {
  const cards = await req(tx('cards').getAll());
  if (!cards.length) { toast('No cards saved yet'); return; }

  busy('Flashcards');
  const enc = new TextEncoder();
  const files = [];
  const rows = [];
  let n = 0;

  for (const c of cards) {
    let media = '';
    if (c.audio) {
      const hit = await req(tx('tts').get(c.audio)).catch(() => null);
      if (hit && hit.blob) {
        const name = `lingua_${c.id}.mp3`;
        files.push({ name: 'media/' + name,
                     bytes: new Uint8Array(await hit.blob.arrayBuffer()) });
        media = `[sound:${name}]`;
        n++;
      }
    }
    // Tab separated, with tabs and newlines stripped from the fields so a
    // multi-line note cannot break the row structure.
    const clean = v => (v || '').replace(/[\t\r\n]+/g, ' ').trim();
    rows.push([clean(c.front), clean(c.back) + (media ? ' ' + media : ''),
               clean(c.note), clean(c.source)].join('\t'));
  }

  files.unshift({ name: 'cards.tsv', bytes: enc.encode(rows.join('\n') + '\n') });
  files.unshift({ name: 'README.txt', bytes: enc.encode(
    'Lingua flashcard export\n\n'
    + 'Fields: Front(English)  Back(Spanish)  Note  Source\n'
    + 'Audio files are in media/ and referenced as [sound:...].\n\n'
    + 'To build an Anki deck, run on the PC:\n'
    + '    python anki.py path/to/this.zip\n') });

  const blob = zipStore(files);
  const name = 'lingua-cards-' + new Date().toISOString().slice(0, 10) + '.zip';
  const file = new File([blob], name, { type: 'application/zip' });

  hideSheet();
  if (navigator.canShare && navigator.canShare({ files: [file] })) {
    try { await navigator.share({ files: [file], title: name }); } catch (e) { return; }
  } else {
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = name; a.click();
    setTimeout(() => URL.revokeObjectURL(url), 5000);
  }
  toast(`${cards.length} cards, ${n} with audio`);
}

/* ---- straight to Anki on the phone ----
 *
 * The batch export above goes through the PC, which is friction when you are
 * studying away from it. These two put the pieces on the phone directly:
 * text on the clipboard, audio in Files. Anki Mobile's own card editor takes
 * it from there.
 */

async function copyText(text, btn) {
  const label = btn ? btn.textContent : null;
  try {
    await navigator.clipboard.writeText(text);
  } catch (e) {
    // Older WebKit, or a context where the async API is refused.
    const ta = document.createElement('textarea');
    ta.value = text;
    ta.style.cssText = 'position:fixed;opacity:0';
    document.body.appendChild(ta);
    ta.select();
    try { document.execCommand('copy'); } catch (e2) { toast('Could not copy'); }
    ta.remove();
  }
  if (btn) {
    btn.textContent = 'Copied ✓';
    setTimeout(() => { btn.textContent = label; }, 1400);
  }
}

/* Filename from the phrase itself, so it is recognisable in Anki's file
   picker rather than being one of a dozen identical names. */
function audioName(text) {
  const base = text.toLowerCase()
    .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 40) || 'frase';
  return base + '.mp3';
}

async function saveAudioFile(text, btn) {
  if (!getTtsKey() || !getVoice()) {
    toast('Set an ElevenLabs key and voice in Settings', 4000);
    return;
  }
  const label = btn ? btn.textContent : null;
  if (btn) btn.textContent = '…';

  const k = await hashKey(text, getVoice());
  let hit = await req(tx('tts').get(k)).catch(() => null);
  if (!hit) {
    await speak(text, null);                       // generates and caches
    hit = await req(tx('tts').get(k)).catch(() => null);
  }
  if (!hit || !hit.blob) {
    if (btn) btn.textContent = label;
    return;                                        // speak() already explained why
  }

  const file = new File([hit.blob], audioName(text), { type: 'audio/mpeg' });
  try {
    if (navigator.canShare && navigator.canShare({ files: [file] })) {
      await navigator.share({ files: [file] });
    } else {
      const url = URL.createObjectURL(hit.blob);
      const a = document.createElement('a');
      a.href = url; a.download = file.name; a.click();
      setTimeout(() => URL.revokeObjectURL(url), 5000);
    }
  } catch (e) { /* cancelled */ }
  if (btn) btn.textContent = label;
}

/* ---- card list ---- */

async function showCards() {
  const cards = (await req(tx('cards').getAll())).reverse();
  if (!cards.length) {
    showSheet(`<div class="hw">Flashcards</div>
      <div class="lm">None saved yet. Use "Save card" after a practice answer.</div>
      <div class="row"><button id="sheetclose">Close</button></div>`);
    $('sheetclose').onclick = hideSheet;
    return;
  }
  showSheet(`<div class="hw">${cards.length} flashcard${cards.length === 1 ? '' : 's'}</div>
    <div class="lm">Exported to Anki; not reviewed here</div>
    ${cards.slice(0, 40).map(c => `
      <div class="nt" style="display:flex;align-items:flex-start;gap:8px">
        <div style="flex:1">
          <div>${esc(c.back)}${c.audio ? ' ♪' : ''}</div>
          <div style="opacity:.7">${esc(c.front)}</div>
        </div>
        <button data-del="${c.id}" style="padding:2px 8px">×</button>
      </div>`).join('')}
    ${cards.length > 40 ? `<div class="lm">…and ${cards.length - 40} more</div>` : ''}
    <div class="row"><button id="cexport">Export for Anki</button></div>`);

  $('cexport').onclick = exportCards;
  $('sheet').querySelectorAll('[data-del]').forEach(b => b.onclick = async () => {
    await req(tx('cards', 'readwrite').delete(+b.dataset.del));
    showCards();
  });
}

/* =====================================================================
 * Study log.
 *
 * Everything here is derived from the event log by replay - nothing extra
 * is stored. That means the per-day history covers every session ever
 * logged, including the ones recorded before this view existed, and it
 * survives the same backup and restore as vocabulary.
 * ===================================================================== */

const STREAK_MIN = 60;        // seconds in a day for it to count as studied

function dayTotal(key) {
  const d = S.days[key];
  return d ? d.read + d.listen : 0;
}

function recentDays(n) {
  const out = [];
  const now = new Date();
  for (let i = n - 1; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const key = dayKey(d.getTime());
    out.push({ key, date: d, ...(S.days[key] || { read: 0, listen: 0 }) });
  }
  return out;
}

function streak() {
  // Counts back from today; a day with nothing today does not break a streak
  // until tomorrow, so an evening session still counts.
  let n = 0;
  const now = new Date();
  for (let i = 0; i < 400; i++) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const t = dayTotal(dayKey(d.getTime()));
    if (t >= STREAK_MIN) n++;
    else if (i > 0) break;
  }
  return n;
}

function totals() {
  let read = 0, listen = 0, days = 0;
  for (const k in S.days) {
    read += S.days[k].read;
    listen += S.days[k].listen;
    if (dayTotal(k) >= STREAK_MIN) days++;
  }
  return { read, listen, all: read + listen, days };
}

function barChart(days) {
  const max = Math.max(60, ...days.map(d => d.read + d.listen));
  const H = 64;
  const rows = days.map(d => {
    const total = d.read + d.listen;
    const h = total ? Math.max(3, Math.round(total / max * H)) : 0;
    const lh = total ? Math.round(d.listen / max * H) : 0;
    const label = d.date.toLocaleDateString(undefined, { weekday: 'narrow' });
    const title = `${d.key}: ${hm(total)}`
      + (d.listen ? ` (${hm(d.read)} read, ${hm(d.listen)} listen)` : '');
    return `<div style="flex:1;display:flex;flex-direction:column;
                        align-items:center;gap:4px" title="${esc(title)}">
      <div style="height:${H}px;display:flex;flex-direction:column;
                  justify-content:flex-end;width:100%;max-width:18px">
        ${lh ? `<div style="height:${lh}px;background:var(--learn-b);
                  border-radius:2px 2px 0 0"></div>` : ''}
        ${h - lh > 0 ? `<div style="height:${h - lh}px;background:var(--accent);
                  border-radius:${lh ? '0' : '2px 2px'} 0 0"></div>` : ''}
      </div>
      <div style="font-size:10px;color:var(--dim)">${esc(label)}</div>
    </div>`;
  }).join('');

  return `<div style="display:flex;align-items:flex-end;gap:3px;margin:10px 0">
    ${rows}</div>
    <div style="font-size:11px;color:var(--dim);display:flex;gap:12px">
      <span><span style="display:inline-block;width:8px;height:8px;
        background:var(--accent);border-radius:2px"></span> read along</span>
      <span><span style="display:inline-block;width:8px;height:8px;
        background:var(--learn-b);border-radius:2px"></span> listening</span>
      <span style="margin-left:auto">peak ${hm(max)}</span>
    </div>`;
}

async function showLog() {
  const t = totals();
  const today = dayTotal(dayKey(Date.now()));
  const week = recentDays(7).reduce((a, d) => a + d.read + d.listen, 0);
  const st = streak();

  const lessons = (await req(tx('lessons').getAll()))
    .map(r => ({ title: r.man.title, secs: S.studied[r.man.id] || 0 }))
    .filter(x => x.secs > 30)
    .sort((a, b) => b.secs - a.secs);

  const cell = (big, small) =>
    `<div style="flex:1"><div style="font-size:22px">${big}</div>
     <div style="font-size:11px;color:var(--dim)">${small}</div></div>`;

  showSheet(`
    <div class="hw">Study log</div>
    <div style="display:flex;gap:10px;margin:12px 0 4px">
      ${cell(hm(today), 'today')}
      ${cell(hm(week), 'last 7 days')}
      ${cell(hm(t.all), 'all time')}
      ${cell(st + (st === 1 ? ' day' : ' days'), 'streak')}
    </div>
    ${barChart(recentDays(14))}
    <div class="lm" style="margin-top:12px">
      ${t.days} day${t.days === 1 ? '' : 's'} studied ·
      ${hm(t.read)} reading · ${hm(t.listen)} listening
    </div>
    ${lessons.length ? `<div class="lm" style="margin-top:12px">By lesson</div>`
      + lessons.slice(0, 15).map(l => `
        <div class="nt" style="display:flex;gap:10px">
          <span style="flex:1">${esc(l.title)}</span>
          <span style="color:var(--fg)">${hm(l.secs)}</span>
        </div>`).join('') : ''}
    <div class="row">
      <button id="logcsv">Export CSV</button>
      <button id="sheetclose">Close</button>
    </div>`);

  $('sheetclose').onclick = hideSheet;
  $('logcsv').onclick = exportLog;
}

async function exportLog() {
  const keys = Object.keys(S.days).sort();
  const rows = ['date,minutes,read_minutes,listen_minutes'];
  for (const k of keys) {
    const d = S.days[k];
    rows.push([k, Math.round((d.read + d.listen) / 60),
               Math.round(d.read / 60), Math.round(d.listen / 60)].join(','));
  }
  const blob = new Blob([rows.join('\n') + '\n'], { type: 'text/csv' });
  const name = 'lingua-log-' + dayKey(Date.now()) + '.csv';
  const file = new File([blob], name, { type: 'text/csv' });
  if (navigator.canShare && navigator.canShare({ files: [file] })) {
    try { await navigator.share({ files: [file], title: name }); return; }
    catch (e) { return; }
  }
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url; a.download = name; a.click();
  setTimeout(() => URL.revokeObjectURL(url), 5000);
}
