"""
PHASE 0 SPIKE — is gpt-4o-transcribe good enough on your candidates' English?

The whole Viva feature rests on one unproven assumption: that transcription of
the English your actual candidates speak comes back clean. This script is the
cheapest possible way to find out — before a single line of feature code.

It is deliberately standalone: it imports nothing from the app, writes nothing
to the database, and calls the OpenAI REST endpoint directly with httpx (an
existing dependency) so it can't trip over the pinned openai SDK version.

WHAT TO DO
    1. Record 5-10 real answers the way your candidates actually speak — a mix
       of clean English and the accented/Banglish English you expect in
       practice. Phone voice-memo or video is fine (m4a, mp3, mp4, wav, webm).
    2. Copy them to the droplet and run this on each. Read the transcripts.
    3. If they're clean, we build Phase 1. If they're noise, we've spent ~$0.10
       and a few minutes instead of a month.

USAGE (on the droplet — it has the venv and the .env key)
    cd ~/app
    venv/bin/python scripts/spike_transcribe.py answer1.m4a
    venv/bin/python scripts/spike_transcribe.py answer1.m4a --model gpt-4o-mini-transcribe
    venv/bin/python scripts/spike_transcribe.py answer1.m4a --minutes 2   # for a cost line

    --model   gpt-4o-transcribe (default) | gpt-4o-mini-transcribe (half price)
              Run a file through both and compare — mini is $0.003/min vs
              $0.006/min, and on clean audio it may be indistinguishable.
    --minutes optional clip length, only used to print an estimated cost.

NOTES
    * The transcription API hard-caps the file at 25 MB. Your constrained
      recordings (~20 MB for 2 min) fit; a raw phone video may not — the script
      warns before spending anything.
    * Accepted formats: flac, m4a, mp3, mp4, mpeg, mpga, oga, ogg, wav, webm.
"""

import argparse
import os
import sys
import time

import httpx
from dotenv import load_dotenv

load_dotenv()

API_URL = "https://api.openai.com/v1/audio/transcriptions"
MAX_BYTES = 25 * 1024 * 1024  # OpenAI's hard limit for the transcription endpoint
PRICE_PER_MIN = {"gpt-4o-transcribe": 0.006, "gpt-4o-mini-transcribe": 0.003, "whisper-1": 0.006}
OK_EXT = {".flac", ".m4a", ".mp3", ".mp4", ".mpeg", ".mpga", ".oga", ".ogg", ".wav", ".webm"}


def human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f}{unit}"
        n /= 1024
    return f"{n:.1f}TB"


def main() -> int:
    ap = argparse.ArgumentParser(description="Phase 0 transcription spike")
    ap.add_argument("file", help="local audio/video file to transcribe")
    ap.add_argument("--model", default="gpt-4o-transcribe",
                    choices=["gpt-4o-transcribe", "gpt-4o-mini-transcribe", "whisper-1"])
    ap.add_argument("--minutes", type=float, default=None,
                    help="clip length in minutes, only used to estimate cost")
    args = ap.parse_args()

    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        print("OPENAI_API_KEY not set (run this on the droplet, where .env has it).")
        return 1

    if not os.path.exists(args.file):
        print(f"File not found: {args.file}")
        return 1

    size = os.path.getsize(args.file)
    ext = os.path.splitext(args.file)[1].lower()
    print(f"\nfile   : {args.file}")
    print(f"size   : {human(size)}")
    print(f"model  : {args.model}")

    if ext not in OK_EXT:
        print(f"\n⚠ Extension {ext!r} isn't in the accepted set {sorted(OK_EXT)}.")
        print("  The API may reject it. Convert to m4a/mp3/mp4 and retry.")
    if size > MAX_BYTES:
        print(f"\n✗ File is {human(size)} — over the 25 MB API limit. Not sending (would fail).")
        print("  This is exactly why Viva constrains the recording bitrate. For the")
        print("  spike, record a shorter/lower-bitrate clip, or extract the audio track.")
        return 1

    print("\ntranscribing… (a 2-min clip is usually 10-30s)\n")
    t0 = time.time()
    try:
        with open(args.file, "rb") as fh:
            resp = httpx.post(
                API_URL,
                headers={"Authorization": f"Bearer {key}"},
                data={"model": args.model, "response_format": "json"},
                files={"file": (os.path.basename(args.file), fh)},
                timeout=180.0,
            )
    except httpx.TimeoutException:
        print("✗ Timed out after 180s. Try a shorter clip.")
        return 1
    except Exception as e:
        print(f"✗ Request failed: {e}")
        return 1
    elapsed = time.time() - t0

    if resp.status_code != 200:
        print(f"✗ HTTP {resp.status_code}")
        body = resp.text[:500]
        print(f"  {body}")
        if resp.status_code == 401:
            print("  → API key rejected. Check OPENAI_API_KEY.")
        elif resp.status_code == 400 and "model" in body.lower():
            print(f"  → The account may not have access to {args.model}, or the name changed.")
        elif resp.status_code == 413:
            print("  → File too large for the endpoint.")
        return 1

    try:
        text = resp.json().get("text", "")
    except Exception:
        text = resp.text

    words = len(text.split())
    print("─" * 64)
    print(text.strip() or "(empty transcript — the model heard nothing usable)")
    print("─" * 64)
    print(f"\nwords    : {words}")
    print(f"chars    : {len(text)}")
    print(f"elapsed  : {elapsed:.1f}s")
    if args.minutes:
        cost = args.minutes * PRICE_PER_MIN[args.model]
        print(f"cost est : ${cost:.4f}  ({args.minutes} min × ${PRICE_PER_MIN[args.model]}/min)")
    print("\nRead it as a recruiter would: could you score this answer from this text?")
    print("Clean → build Phase 1. Noise → the feature needs an English-only guardrail.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
