# Lingua — making a lesson

Reference for the whole pipeline, from a source video to a lesson on the phone.

---

## Quick version

```bash
lingua                                        # activate the project
cd media
yt-dlp -f "bv*[height<=1080]+ba/b" --merge-output-format mp4 \
  -o "NAME.%(ext)s" "URL"
ffmpeg -i NAME.mp4 -c copy -movflags +faststart tmp.mp4 && mv tmp.mp4 NAME.mp4

cd ~/lingua
python run.py media/NAME.mp4                  # ASR → segment → annotate → translate
python package.py media/NAME.translated.json  # transcode + zip
```

Result: `packages/NAME.lpkg`

Pick a short `NAME` with no spaces — it becomes the filename at every stage.

---

## Step by step

### 1. Get the media

**From YouTube.** The format string matters: `bv*+ba` takes the best video and
best audio *independently*. Selecting by resolution alone bundles low-bitrate
audio with low-res video, which handicaps the transcription before it starts.

```bash
cd ~/lingua/media
yt-dlp -f "bv*[height<=1080]+ba/b" --merge-output-format mp4 \
  -o "NAME.%(ext)s" "URL"
```

**Then remux for fast seeking.** Puts the index at the front of the file so a
player can jump without downloading the whole thing. Stream copy, no re-encode,
a second or two.

```bash
ffmpeg -i NAME.mp4 -c copy -movflags +faststart tmp.mp4 && mv tmp.mp4 NAME.mp4
```

**Already have the file?** Copy it into `~/lingua/media/`. Reading across
`/mnt/c/` is slow enough to matter on a long file.

**Check what you got:**

```bash
ffprobe -v error -select_streams a:0 \
  -show_entries stream=codec_name,sample_rate,bit_rate \
  -of default=noprint_wrappers=1 NAME.mp4
```

Audio at 128 kbps or better is what you want.

### 2. Run the pipeline

```bash
cd ~/lingua
python run.py media/NAME.mp4
```

Four stages, run in order:

| Stage | What it does | Where | Cost |
|---|---|---|---|
| `ingest` | Extract audio, WhisperX ASR, forced alignment | GPU | free |
| `segment` | Restore punctuation, split into sentences | API | ~$0.10 / 12 min |
| `annotate` | Lemma, part of speech, gender via spaCy | local | free |
| `translate` | Sentence translations, per-word glosses | API | ~$0.15–0.45 |

Each stage writes a file and is skipped if it is already newer than its input,
so a crash resumes rather than restarting. API responses are cached on disk —
re-running after a failure does not pay twice.

**Preview cost first:**

```bash
python translate.py media/NAME.annotated.json --dry-run
```

**Force a full re-run:**

```bash
python run.py media/NAME.mp4 --force
```

### 3. Check it before packaging

```bash
cd ~/lingua/media && python serve.py . 8000
```

Open `http://localhost:8000/preview.html?lesson=NAME`.

Cheaper to catch a bad transcript here than after it is on the phone. Look at:

- Do the sentences break sensibly?
- Does the highlighting track the audio?
- Anything in the flagged-words list that is actually wrong?

Diagnostics if something looks off:

```bash
python check.py media/NAME.transcript.json
```

Coverage near 90% is normal. Much lower means either long silences (fine —
check the gap list) or VAD dropping speech (not fine).

### 4. Package

```bash
cd ~/lingua
python package.py media/NAME.translated.json
```

Defaults to 480p / CRF 30, which is about 45 MB for 12 minutes and looks fine
on a phone.

**Variations:**

```bash
# Higher quality for footage worth watching
python package.py media/NAME.translated.json --height 720 --crf 26
python package.py media/NAME.translated.json --height 1080 --crf 23

# Audio only — about 11 MB, for re-listening to something already watched
python package.py media/NAME.translated.json --audio-only

# GPU encoder: much faster, slightly larger files
python package.py media/NAME.translated.json --nvenc

# Audiobook chapter — groups with its siblings in the library
python package.py media/ch03.translated.json --series "Book Title" --chapter 3
```

Video and audio-only builds of the same source are separate library items with
separate ids, so both can be installed at once. They share vocabulary, since
that is keyed by lemma on the device.

### 5. Onto the phone

1. Copy the `.lpkg` from `~/lingua/packages/` to iCloud Drive on Windows
2. On the phone: open Lingua → **•••** → **Import lesson**
3. Pick the file from Files

Files are large, so give iCloud time to finish uploading before importing.

---

## Songs and audiobooks: supporting text

Speech recognition is weakest on music — instruments bleed into the vocal,
choruses are the densest part of the mix — and it is needlessly error-prone on
audiobooks when the exact text already exists. Two tools fix this, and they
stack.

**Supply the text.** Put it beside the audio with the same name:

```
media/mi_cancion.mp3
media/mi_cancion.txt      <- lyrics, one line per sung line
```

`run.py` and `make.py` find it automatically. The words then come from your
file, and speech recognition is used only to work out roughly when each line
happens. Your line breaks become the sentences, and the paid segmentation
stage is skipped.

For lyrics, **write every repetition out in full, in the order sung.** A line
like `Coro (x2)` cannot be expanded by the aligner and throws off every line
after it. Section headers such as `[Coro]` or `(Verso 2)` on their own line are
ignored, so they can stay.

For ebook chapters, paste the chapter's prose. It is detected as prose and
split into sentences rather than lines.

**Isolate the vocals** for anything with music:

```bash
python make.py media/mi_cancion.mp3 --music
python make.py media/*.mp3 --music          # a folder of songs
```

This works with or without supporting text. Without lyrics it still improves
recognition noticeably; with them, it improves the timing.

**Check the agreement figure** in the output — `ASR agreed with the text on
NN% of words`. Songs typically land somewhere around 40–80%. Below about 25%
usually means the wrong text for this audio, or a repeat that was not written
out.

**One-time setup** for vocal separation:

```bash
lingua
uv pip install demucs
python -c "import torch; print(torch.__version__, torch.cuda.is_available())"
```

That last line should still print a `+cu128` version and `True`. If installing
demucs changed torch, restore it:

```bash
uv pip install torch==2.8.0 torchaudio==2.8.0 \
  --index-url https://download.pytorch.org/whl/cu128
```

---

## Reference

### Rough cost and time, per 20 minutes of video

| | |
|---|---|
| ASR + alignment | ~1 min GPU, free |
| Segmentation | ~$0.19 |
| Translation + glosses | ~$0.20–0.45, falling as `vocab.db` grows |
| Transcode | 2–4 min CPU (seconds with `--nvenc`) |

Vocabulary reuse across lessons is real but gradual — roughly 15–20% overlap
early on, climbing as the database fills. Do not expect the cost to collapse
after a handful of lessons.

### Files produced

```
media/NAME.mp4                  source
media/NAME.transcript.json      words + timestamps
media/NAME.segmented.json       + punctuation and sentences
media/NAME.annotated.json       + lemma, POS, gender
media/NAME.translated.json      + translations and glosses
media/.cache/                   API responses (safe to delete, costs money to rebuild)
packages/NAME.lpkg              the deliverable
vocab.db                        every lemma ever glossed — do not delete
```

`vocab.db` is the one file worth backing up on the PC side. It is what makes
each new lesson cheaper than the last.

### Updating the PWA

```bash
lingua && cd pwa
git add -A && git commit -m "what changed" && git push
```

GitHub Pages redeploys in a minute or two. On the phone, **fully close the app
from the app switcher** before relaunching — a running instance keeps the old
JavaScript in memory.

### Flashcards to Anki

Two routes. The phone one needs no PC.

**On the phone, one card at a time.** After a practice answer, or on any
sentence you tap in a lesson:

- **Copy ES** / **Copy EN** put the text on the clipboard
- **Save audio** generates the mp3 and opens the share sheet - choose
  *Save to Files*. The filename comes from the phrase itself
  (`me_regala_la_cuenta.mp3`), so it is easy to find later.

Then in Anki Mobile: new card, paste the two fields, tap the paperclip to
attach the audio from Files. Done without leaving the phone.

Audio is cached, so saving a phrase you have already played costs nothing
against the ElevenLabs allowance.

**In a batch, through the PC.** Still there if you ever want it:
**Add to card batch** on each answer, then **•••** -> **Flashcards** ->
**Export for Anki**, and on the PC:

```bash
uv pip install genanki        # once
python anki.py ~/Downloads/lingua-cards-2026-10-08.zip
```

That writes a `.apkg` beside the zip - one-step import, audio attached
automatically. Cards are keyed on their Spanish text, so a later export
updates notes you already have rather than duplicating them.

Batched cards are included in the memory backup, so they survive a reinstall
even before you export.

### Practice CLI

The same features as the app, from the desktop:

```bash
python tutor.py say "Could I get the check when you have a moment?"
python tutor.py fix "Quiero un mesa para dos personas"
python tutor.py chat media/NAME.translated.json --at 40
```

### Common problems

**`ModuleNotFoundError: torch`** — the venv is not active. Run `lingua`.

**Import greyed out on iPhone** — should be fixed; the picker no longer
filters by extension. If it recurs, rename the file to `.zip`.

**`TTS error 402`** — the free ElevenLabs tier cannot use Voice Library voices
through the API. Pick one from the "Works on any plan" group, or upgrade.

**Video will not seek in the browser preview** — use `serve.py`, not
`python -m http.server`. The latter cannot serve byte ranges.

**Whisper invents a word** — check the flagged list after segmentation.
Repeated inconsistent spellings of the same sound are the reliable signal.

**Song highlighting drifts or vanishes** — add the lyrics as `NAME.txt` and
use `--music`. Check the agreement percentage; a low figure usually means a
repeated chorus was not written out in full.
