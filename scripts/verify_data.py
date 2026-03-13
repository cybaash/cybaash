#!/usr/bin/env python3
"""
verify_data.py — Integrity checker for the split portfolio data files
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

data.json has been split into 5 smaller files:
  portfolio/data_main.json     — about, contact, experience, skills, projects, flags
  portfolio/data_creds_1.json  — credentials batch 1
  portfolio/data_creds_2.json  — credentials batch 2
  portfolio/data_creds_3.json  — credentials batch 3
  portfolio/data_creds_4.json  — credentials batch 4

This script verifies all five: valid JSON, correct keys, no LFS pointers,
no duplicate credential IDs across batches.

Usage:
  python scripts/verify_data.py
  python scripts/verify_data.py --chunk-size 262144 --strict

Exit codes:
  0 — all files valid
  1 — any file missing, empty, truncated, or invalid JSON
  2 — argument error
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path


# ─── ANSI colours ────────────────────────────────────────────────────────────
_IS_TTY = sys.stdout.isatty()

def _c(code, text): return f"\033[{code}m{text}\033[0m" if _IS_TTY else text

GREEN  = lambda t: _c("92", t)
RED    = lambda t: _c("91", t)
YELLOW = lambda t: _c("93", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)


# ─── File layout ─────────────────────────────────────────────────────────────
SPLIT_FILES = [
    "portfolio/data_main.json",
    "portfolio/data_creds_1.json",
    "portfolio/data_creds_2.json",
    "portfolio/data_creds_3.json",
    "portfolio/data_creds_4.json",
]
MAIN_REQUIRED_KEYS = {"about", "contact", "skills", "experience", "projects", "flags"}


# ─── Helpers ─────────────────────────────────────────────────────────────────
def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"

def fmt_duration(s):
    return f"{s*1000:.1f} ms" if s < 1 else f"{s:.2f} s"


# ─── Chunked reader ───────────────────────────────────────────────────────────
def stream_file(path, chunk_size):
    hasher    = hashlib.sha256()
    collector = bytearray()
    total     = 0
    t0        = time.monotonic()
    chunks    = 0

    with path.open("rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            hasher.update(chunk)
            collector.extend(chunk)
            total  += len(chunk)
            chunks += 1
            if chunks % 10 == 0:
                elapsed = time.monotonic() - t0
                speed   = total / elapsed if elapsed > 0 else 0
                print(f"  {DIM('›')} read {fmt_bytes(total)}  ({fmt_bytes(int(speed))}/s)", end="\r", flush=True)

    print(" " * 72, end="\r")
    return bytes(collector), hasher.hexdigest(), total


# ─── Validators ──────────────────────────────────────────────────────────────
def check_no_lfs_pointer(raw, path):
    if raw.startswith(b"version https://git-lfs.github.com/spec/"):
        print(RED(f"✖ FAIL: {path} is an LFS pointer, not real content."))
        print(RED("  → Run: git lfs pull"))
        sys.exit(1)

def check_json(raw, path):
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        print(RED(f"✖ FAIL: {path} is not valid UTF-8 — {e}"))
        sys.exit(1)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as e:
        print(RED(f"✖ FAIL: invalid JSON in {path} — {e}"))
        sys.exit(1)
    if not isinstance(data, dict):
        print(RED(f"✖ FAIL: {path} root must be a JSON object, got {type(data).__name__}"))
        sys.exit(1)
    return data

def verify_file(path_str, chunk_size, strict):
    """Verify one file. Returns (data_dict, sha256, size_bytes)."""
    path = Path(path_str)
    print(f"\n  {BOLD(path_str)}")

    if not path.exists():
        print(RED(f"  ✖ FAIL: file not found — {path.resolve()}"))
        sys.exit(1)

    disk_size = path.stat().st_size
    if disk_size == 0:
        print(RED(f"  ✖ FAIL: file is empty (0 bytes)"))
        sys.exit(1)

    raw, sha256, total = stream_file(path, chunk_size)
    check_no_lfs_pointer(raw, path)
    data = check_json(raw, path)

    # File-specific structure checks
    fname = path.name
    if fname == "data_main.json":
        if strict:
            missing = MAIN_REQUIRED_KEYS - set(data.keys())
            if missing:
                print(RED(f"  ✖ FAIL (--strict): data_main.json missing keys: {sorted(missing)}"))
                sys.exit(1)
        print(f"  {GREEN('✔')}  keys     : {sorted(data.keys())}")
    elif fname.startswith("data_creds_"):
        creds = data.get("credentials")
        if not isinstance(creds, list):
            print(RED(f"  ✖ FAIL: {path} must contain a 'credentials' list"))
            sys.exit(1)
        print(f"  {GREEN('✔')}  credentials: {len(creds)}")

    print(f"  {GREEN('✔')}  sha256   : {sha256}")
    print(f"  {GREEN('✔')}  size     : {fmt_bytes(total)}")
    return data, sha256, total


# ─── Main ─────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Verify split portfolio data files.")
    parser.add_argument("--chunk-size", type=int, default=524_288, metavar="BYTES",
                        help="Read chunk size in bytes (default: 524288 = 512 KB)")
    parser.add_argument("--strict", action="store_true",
                        help="Require all top-level keys in data_main.json")
    args = parser.parse_args()

    print(BOLD(f"\n{'━'*60}"))
    print(BOLD("  🔍  Split Data Files — Integrity Verifier"))
    print(BOLD(f"{'━'*60}"))
    print(f"  {DIM('Files :')} {len(SPLIT_FILES)}")
    print(f"  {DIM('Chunk :')} {fmt_bytes(args.chunk_size)}")
    print(f"  {DIM('Strict:')} {args.strict}\n")

    t_total_start = time.monotonic()
    all_cred_ids  = []
    total_creds   = 0
    summary       = []

    for f in SPLIT_FILES:
        data, sha256, size = verify_file(f, args.chunk_size, args.strict)
        summary.append((f, sha256, size))
        if "credentials" in data:
            ids = [c.get("id") for c in data["credentials"]]
            all_cred_ids.extend(ids)
            total_creds += len(ids)

    # Cross-file duplicate ID check
    if all_cred_ids:
        seen   = set()
        dupes  = set()
        for cid in all_cred_ids:
            if cid in seen:
                dupes.add(cid)
            seen.add(cid)
        if dupes:
            print(RED(f"\n  ✖ FAIL: duplicate credential IDs across files: {sorted(dupes)}"))
            sys.exit(1)
        print(f"\n  {GREEN('✔')}  Total credentials (all files): {total_creds}  — no duplicates")

    elapsed = time.monotonic() - t_total_start
    print(f"  {GREEN('✔')}  All {len(SPLIT_FILES)} files verified in {fmt_duration(elapsed)}\n")

    # GitHub Actions step summary
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write("### ✅ Split Data Files — Integrity Report\n\n")
            fh.write("| File | Size | SHA-256 |\n|---|---|---|\n")
            for fname, sha, size in summary:
                fh.write(f"| `{fname}` | {fmt_bytes(size)} | `{sha[:16]}…` |\n")
            fh.write(f"\n**Total credentials:** {total_creds}\n")

    # Write total_creds to GITHUB_OUTPUT if available
    github_output = os.environ.get("GITHUB_OUTPUT")
    if github_output:
        with open(github_output, "a") as fh:
            fh.write(f"total_creds={total_creds}\n")

    print(f"  {GREEN(BOLD('✔  All checks passed.'))}\n")
    print(f"{'━'*60}\n")


if __name__ == "__main__":
    main()
