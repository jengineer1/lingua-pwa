#!/usr/bin/env python3
"""
Test harness for the app's runtime API features.

These are the parts that need network on the phone. Testing them as a CLI
first means the prompts get iterated here, where a round trip is seconds,
rather than in Xcode where it is minutes.

    python tutor.py chat media/video2.translated.json --at 40
    python tutor.py chat media/video2.translated.json --full
    python tutor.py say "I'd like a table for two, please"
    python tutor.py fix "Quiero un mesa para dos personas"

Modes:
  chat   conversational Q&A. Sends a window around the current sentence by
         default; --full sends the whole transcript with prompt caching so
         repeat turns cost a fraction.
  say    English in, natural Spanish out, with a note on why it is phrased
         that way. This is the mode as originally described.
  fix    You attempt Spanish, it corrects. Production practice.
"""

import argparse
import json
import os
import sys
from pathlib import Path

try:
    from dotenv import load_dotenv
except ImportError:
    print("pip install python-dotenv anthropic", file=sys.stderr)
    raise

import anthropic

MODEL = "claude-sonnet-4-5"
WINDOW_BACK = 3
WINDOW_FWD = 2

# Prices per million tokens, for the running cost display.
PRICE_IN, PRICE_OUT = 3.0, 15.0

REGION_NOTE = (
    "The learner's target variety is {region} Spanish, but they listen to "
    "speakers from across Latin America.\n"
    "When producing Spanish for them, use {region} vocabulary and register.\n"
    "When explaining Spanish they are reading or hearing, explain it as the "
    "speaker meant it in their own variety. Name the region when a usage is "
    "regional, and mention the {region} equivalent when it differs.")

CHAT_SYSTEM = """You help an English speaker who is studying {lang} by \
reading and listening to real material.

{region}

You may be given an excerpt of what they are currently reading. Use it when \
the question refers to it, and ignore it when the question is general.

Answer the question asked, briefly. Do not turn every answer into a grammar \
lesson - if they asked what a word means, tell them, and add the grammar only \
when it is what makes the sentence work. Assume intelligence, not knowledge: \
they can follow a real explanation, they just do not know the language yet.

Write in English unless quoting."""

SAY_SYSTEM = """You turn English into natural {lang}.

{region}

Return exactly this, no preamble:

1. The {lang} sentence a native speaker would actually say. Not a literal \
rendering of the English - what someone would say in that situation.
2. A back-translation in English of what you wrote, so the learner can see \
whether the meaning shifted.
3. One or two lines on anything worth noticing: a word choice that differs \
from the obvious one, a structure with no English equivalent, a register \
choice. Skip this if there is nothing genuinely worth saying.

If the English is ambiguous or the situation changes the phrasing, give the \
most likely reading and note the alternative in one line."""

FIX_SYSTEM = """You correct a learner's {lang}.

{region}

Return exactly this, no preamble:

1. VERDICT - one line: correct, understandable but unnatural, or wrong.
2. CORRECTED - what a native speaker would say. If the original was already \
fine, say so and do not invent a change.
3. WHY - what was off and the rule behind it, briefly. Name the error type \
(gender agreement, wrong preposition, ser/estar, false friend) so the pattern \
becomes recognisable rather than a one-off fix.

Be accurate about severity. Do not soften a real error into a style note, and \
do not flag as an error something that is merely a different valid choice. If \
a native speaker would understand it perfectly and just phrase it differently, \
say that."""

def load_lesson(path: Path):
    doc = json.loads(path.read_text(encoding="utf-8"))
    return doc, doc["segments"]


def window(segs, at: int) -> str:
    lo = max(0, at - WINDOW_BACK)
    hi = min(len(segs), at + WINDOW_FWD + 1)
    lines = []
    for s in segs[lo:hi]:
        mark = ">>" if s["id"] == at else "  "
        lines.append(f"{mark} ({s['id']}) {s['text']}")
        if s.get("translation"):
            lines.append(f"        {s['translation']}")
    return "\n".join(lines)


def full_transcript(segs) -> str:
    return "\n".join(f"({s['id']}) {s['text']}" for s in segs)


class Session:
    def __init__(self, client, system, cached_block=None):
        self.client = client
        self.system = system
        self.cached = cached_block
        self.history = []
        self.tin = self.tout = self.tcache_r = self.tcache_w = 0

    def system_blocks(self):
        blocks = [{"type": "text", "text": self.system}]
        if self.cached:
            # Marking the transcript cached means later turns in the session
            # pay a small fraction for re-sending it.
            blocks.append({
                "type": "text",
                "text": self.cached,
                "cache_control": {"type": "ephemeral"},
            })
        return blocks

    def ask(self, text: str, keep_history=True) -> str:
        msgs = self.history + [{"role": "user", "content": text}]
        resp = self.client.messages.create(
            model=MODEL, max_tokens=2000,
            system=self.system_blocks(), messages=msgs)
        out = "".join(b.text for b in resp.content if b.type == "text")

        u = resp.usage
        self.tin += u.input_tokens
        self.tout += u.output_tokens
        self.tcache_r += getattr(u, "cache_read_input_tokens", 0) or 0
        self.tcache_w += getattr(u, "cache_creation_input_tokens", 0) or 0

        if keep_history:
            self.history = msgs + [{"role": "assistant", "content": out}]
        return out

    def cost_line(self) -> str:
        c = (self.tin / 1e6 * PRICE_IN + self.tout / 1e6 * PRICE_OUT
             + self.tcache_w / 1e6 * PRICE_IN * 1.25
             + self.tcache_r / 1e6 * PRICE_IN * 0.1)
        parts = [f"{self.tin:,} in", f"{self.tout:,} out"]
        if self.tcache_w or self.tcache_r:
            parts.append(f"cache {self.tcache_w:,}w/{self.tcache_r:,}r")
        return f"[{', '.join(parts)}  ~${c:.4f}]"


def fmt(lang: str, region: str, template: str) -> str:
    return template.format(lang=lang, region=REGION_NOTE.format(region=region))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["chat", "say", "fix"])
    ap.add_argument("arg", nargs="?", help="lesson path, or the text to work on")
    ap.add_argument("--at", type=int, default=0, help="current sentence id")
    ap.add_argument("--full", action="store_true",
                    help="send the whole transcript, cached")
    ap.add_argument("--lang", default="Spanish")
    ap.add_argument("--region", default="Mexican")
    args = ap.parse_args()

    load_dotenv(Path.cwd() / ".env")
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ANTHROPIC_API_KEY not set (expected in ./.env)", file=sys.stderr)
        return 1
    client = anthropic.Anthropic()

    if args.mode in ("say", "fix"):
        if not args.arg:
            print(f"usage: python tutor.py {args.mode} \"your sentence\"")
            return 1
        system = fmt(args.lang, args.region,
                     SAY_SYSTEM if args.mode == "say" else FIX_SYSTEM)
        s = Session(client, system)
        print()
        print(s.ask(args.arg, keep_history=False))
        print()
        print(s.cost_line())
        return 0

    if not args.arg:
        print(f"usage: python tutor.py {args.mode} <lesson.translated.json>")
        return 1
    doc, segs = load_lesson(Path(args.arg))

    if args.mode == "chat":
        cached = full_transcript(segs) if args.full else None
        s = Session(client, fmt(args.lang, args.region, CHAT_SYSTEM), cached)
        ctx = None if args.full else window(segs, args.at)
        print(f"\nchat about {Path(args.arg).name}"
              + (" [full transcript, cached]" if args.full
                 else f" [window around sentence {args.at}]"))
        print("commands: :at N  move the window   :full  send everything   "
              ":q  quit\n")
        if ctx:
            print(ctx + "\n")
        while True:
            try:
                q = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print()
                break
            if not q:
                continue
            if q == ":q":
                break
            if q.startswith(":at "):
                args.at = int(q.split()[1])
                ctx = window(segs, args.at)
                print("\n" + ctx + "\n")
                continue
            if q == ":full":
                s.cached = full_transcript(segs)
                ctx = None
                print("[now sending the whole transcript]\n")
                continue
            payload = (f"Currently reading:\n{ctx}\n\nQuestion: {q}"
                       if ctx else q)
            print()
            print(s.ask(payload))
            print()
            print(s.cost_line())
            print()
        print(s.cost_line())
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
