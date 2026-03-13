#!/usr/bin/env bash
# scripts/bootstrap_lfs.sh
# ─────────────────────────────────────────────────────────────────────────────
# One-time setup: migrates portfolio/data.json into Git LFS and pushes.
# Run from the ROOT of the repository, e.g.:
#
#   bash scripts/bootstrap_lfs.sh
#
# Requirements: git, git-lfs
#   macOS:   brew install git-lfs
#   Ubuntu:  sudo apt-get install git-lfs
#   Windows: https://git-lfs.com
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RESET='\033[0m'
info()    { echo -e "${GREEN}[LFS]${RESET} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${RESET} $*"; }
error()   { echo -e "${RED}[ERROR]${RESET} $*" >&2; exit 1; }

# ── Pre-flight ────────────────────────────────────────────────────────────────
command -v git     >/dev/null 2>&1 || error "git is not installed."
command -v git-lfs >/dev/null 2>&1 || error "git-lfs is not installed. See https://git-lfs.com"

# Must be run from repo root
[ -f ".gitattributes" ] || error "Run this script from the repository root (where .gitattributes lives)."

DATA_FILE="portfolio/data.json"
[ -f "$DATA_FILE" ] || error "$DATA_FILE not found."

FILE_SIZE=$(stat -c%s "$DATA_FILE" 2>/dev/null || stat -f%z "$DATA_FILE")
info "data.json size: $(numfmt --to=iec-i --suffix=B "$FILE_SIZE" 2>/dev/null || echo "${FILE_SIZE} bytes")"

# ── Step 1: Install LFS hooks into this repo ──────────────────────────────────
info "Installing Git LFS hooks..."
git lfs install

# ── Step 2: Already tracked? ──────────────────────────────────────────────────
if git check-attr filter "$DATA_FILE" | grep -q "filter: lfs"; then
  warn "$DATA_FILE is already tracked by Git LFS. Checking if pointer is committed..."

  # If the object in the index is still the real file (not a pointer), migrate it
  STAGED_SIZE=$(git cat-file -s :"$DATA_FILE" 2>/dev/null || echo 0)
  if [ "$STAGED_SIZE" -gt 200 ]; then
    warn "Real file is still in the index (not yet migrated to LFS). Migrating..."
    git lfs migrate import --include="$DATA_FILE" --yes
    info "Migration complete."
  else
    info "LFS pointer already in place. Nothing to migrate."
  fi
else
  # ── Step 3: Track the file ──────────────────────────────────────────────────
  info "Tracking $DATA_FILE with Git LFS..."
  git lfs track "$DATA_FILE"

  # ── Step 4: Migrate existing history ────────────────────────────────────────
  info "Migrating existing history to LFS (this rewrites commits — only do this once)..."
  git lfs migrate import --include="$DATA_FILE" --yes
  info "History migration complete."
fi

# ── Step 5: Stage .gitattributes ─────────────────────────────────────────────
info "Staging .gitattributes..."
git add .gitattributes

# ── Step 6: Verify the pointer is correct ─────────────────────────────────────
info "Verifying LFS pointer..."
git lfs status

# ── Step 7: Commit if there are staged changes ────────────────────────────────
if ! git diff --cached --quiet; then
  info "Committing LFS configuration..."
  git commit -m "chore: enable Git LFS for data.json [skip ci]"
else
  info "Nothing new to commit (already clean)."
fi

# ── Step 8: Push (LFS objects first, then refs) ───────────────────────────────
info "Pushing LFS objects and branch to origin/main..."
git push --force-with-lease origin main

info ""
info "✅ Done! data.json is now tracked by Git LFS."
info "   Every future 'git push' will upload it automatically."
info "   GitHub Actions will pull the real bytes via 'lfs: true' in actions/checkout."
