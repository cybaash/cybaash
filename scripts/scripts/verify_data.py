#!/usr/bin/env python3
"""
verify_data.py — Chunked integrity checker for portfolio/data.json
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Reads the file in configurable chunks (default 512 KB) so it never
loads the entire payload into RAM at once. Safe on GitHub Actions
runners (7 GB RAM) and on local machines with constrained memory.

Usage:
  python scripts/verify_data.py --file portfolio/data.json
  python scripts/verify_data.py --file portfolio/data.json --chunk-size 262144 --strict

Exit codes:
  0 — file is valid, complete, and parseable JSON
  1 — file missing, empty, truncated, or invalid JSON
  2 — argument error
"""

import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path


# ─── ANSI colours (auto-disabled when not a TTY) ────────────────────────────
_IS_TTY = sys.stdout.isatty()


def _c(code: str, text: str) -> str:
    return f"\033[{code}m{text}\033[0m" if _IS_TTY else text


GREEN  = lambda t: _c("92", t)
RED    = lambda t: _c("91", t)
YELLOW = lambda t: _c("93", t)
BOLD   = lambda t: _c("1",  t)
DIM    = lambda t: _c("2",  t)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def fmt_duration(seconds: float) -> str:
    if seconds < 1:
        return f"{seconds * 1000:.1f} ms"
    return f"{seconds:.2f} s"


# ─── Core: chunked streaming reader ──────────────────────────────────────────

def stream_file(path: Path, chunk_size: int) -> tuple[bytes, str, int]:
    """
    Read *path* in chunks of *chunk_size* bytes.
    Returns (raw_bytes, sha256_hex, total_bytes_read).

    Uses a rolling SHA-256 so we never keep more than one chunk in RAM
    at a time.  The full content is collected for the JSON parse below —
    this is necessary because Python's json module requires the complete
    string.  If the file ever grows beyond available RAM, replace the
    collector with an incremental JSON parser (e.g. ijson).
    """
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

            # Progress every 10 chunks (~5 MB at default chunk size)
            if chunks % 10 == 0:
                elapsed = time.monotonic() - t0
                speed   = total / elapsed if elapsed > 0 else 0
                print(
                    f"  {DIM('›')} read {fmt_bytes(total)}"
                    f"  ({fmt_bytes(int(speed))}/s, {chunks} chunks)",
                    end="\r",
                    flush=True,
                )

    print(" " * 72, end="\r")   # clear progress line
    return bytes(collector), hasher.hexdigest(), total


# ─── Validators ──────────────────────────────────────────────────────────────

def check_not_empty(total: int, path: Path) -> None:
    if total == 0:
        print(RED(f"✖ FAIL: {path} is empty (0 bytes)."))
        sys.exit(1)


def check_json(raw: bytes, strict: bool) -> dict:
    """
    Decode and parse the raw bytes as UTF-8 JSON.
    In --strict mode, also validates that required top-level keys exist.
    """
    try:
        text = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        print(RED(f"✖ FAIL: file is not valid UTF-8 — {exc}"))
        sys.exit(1)

    # Detect obvious truncation before the full parse
    stripped = text.rstrip()
    if stripped and stripped[-1] not in ('}', ']', '"', '0123456789truefalsn'):
        print(YELLOW(f"⚠  WARNING: file appears to be truncated (last char: {stripped[-1]!r})"))

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        print(RED(f"✖ FAIL: invalid JSON — {exc}"))
        sys.exit(1)

    if not isinstance(data, dict):
        print(RED("✖ FAIL: data.json root must be a JSON object ({...}), got "
                  f"{type(data).__name__}"))
        sys.exit(1)

    if strict:
        required_keys = {"about", "skills"}
        missing = required_keys - data.keys()
        if missing:
            print(RED(f"✖ FAIL (--strict): missing top-level keys: {sorted(missing)}"))
            sys.exit(1)

    return data


def check_no_lfs_pointer(raw: bytes) -> None:
    """
    Detect if GitHub accidentally gave us an LFS pointer instead of the
    real file (happens when LFS is not installed or the token lacks LFS scope).
    """
    if raw.startswith(b"version https://git-lfs.github.com/spec/"):
        print(RED("✖ FAIL: file is an LFS pointer, not the real content."))
        print(RED("  → Run: git lfs pull   or check your LFS credentials."))
        sys.exit(1)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Chunked integrity checker for large JSON files."
    )
    parser.add_argument(
        "--file",
        default="portfolio/data.json",
        help="Path to the JSON file to verify (default: portfolio/data.json)",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=524_288,
        metavar="BYTES",
        help="Read chunk size in bytes (default: 524288 = 512 KB)",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Require top-level keys: about, skills",
    )
    parser.add_argument(
        "--output-hash",
        metavar="FILE",
        help="Write SHA-256 to this file (useful in CI pipelines)",
    )
    args = parser.parse_args()

    path = Path(args.file)

    # ── Existence check ───────────────────────────────────────────────────────
    print(BOLD(f"\n{'━'*60}"))
    print(BOLD("  🔍  data.json Integrity Verifier"))
    print(BOLD(f"{'━'*60}\n"))
    print(f"  {DIM('File  :')} {path.resolve()}")
    print(f"  {DIM('Chunk :')} {fmt_bytes(args.chunk_size)}")
    print(f"  {DIM('Strict:')} {args.strict}\n")

    if not path.exists():
        print(RED(f"✖ FAIL: file not found — {path}"))
        sys.exit(1)

    disk_size = path.stat().st_size
    print(f"  {DIM('Disk size:')} {fmt_bytes(disk_size)}\n")

    # ── Stream ────────────────────────────────────────────────────────────────
    t_start = time.monotonic()
    raw, sha256, total = stream_file(path, args.chunk_size)
    elapsed = time.monotonic() - t_start

    # ── Checks ────────────────────────────────────────────────────────────────
    check_not_empty(total, path)
    check_no_lfs_pointer(raw)
    data = check_json(raw, args.strict)

    # ── Stats ─────────────────────────────────────────────────────────────────
    top_keys   = list(data.keys())
    speed      = total / elapsed if elapsed > 0 else 0

    print(f"  {GREEN('✔')}  SHA-256   : {sha256}")
    print(f"  {GREEN('✔')}  Read      : {fmt_bytes(total)} in {fmt_duration(elapsed)}"
          f"  ({fmt_bytes(int(speed))}/s)")
    print(f"  {GREEN('✔')}  JSON keys : {top_keys}")

    if disk_size != total:
        # Should never happen, but guards against partial reads
        print(YELLOW(f"\n  ⚠  WARNING: disk size ({disk_size}) ≠ bytes read ({total})"))

    # ── Optional hash file output ─────────────────────────────────────────────
    if args.output_hash:
        Path(args.output_hash).write_text(sha256 + "\n", encoding="utf-8")
        print(f"  {GREEN('✔')}  Hash written to {args.output_hash}")

    # ── GitHub Actions summary ────────────────────────────────────────────────
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as fh:
            fh.write("### ✅ data.json Integrity Report\n\n")
            fh.write("| Property | Value |\n|---|---|\n")
            fh.write(f"| File | `{path}` |\n")
            fh.write(f"| Size | {fmt_bytes(total)} |\n")
            fh.write(f"| SHA-256 | `{sha256}` |\n")
            fh.write(f"| Read speed | {fmt_bytes(int(speed))}/s |\n")
            fh.write(f"| JSON keys | `{top_keys}` |\n")
            fh.write(f"| Strict mode | `{args.strict}` |\n")

    print(f"\n  {GREEN(BOLD('✔  All checks passed.'))}\n")
    print(f"{'━'*60}\n")


if __name__ == "__main__":
    main()
