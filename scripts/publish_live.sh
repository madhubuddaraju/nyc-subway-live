#!/usr/bin/env bash
# Force-push the live board to the orphan branch `live-data` as a single commit, so the page always reads the
# newest files and the repository history doesn't grow with every update.
set -euo pipefail
src="$1"
auth=$(git config --get http.https://github.com/.extraheader || true)
tmp=$(mktemp -d)
cp "$src"/*.json "$tmp"/
cd "$tmp"
git init -q -b live-data
git config user.name "github-actions[bot]"
git config user.email "41898699+github-actions[bot]@users.noreply.github.com"
git add -A
git commit -qm "Live board $(date -u +%Y-%m-%dT%H:%MZ)"
git -c http.https://github.com/.extraheader="$auth" push -qf "https://github.com/${GITHUB_REPOSITORY}.git" live-data
echo "published $(ls *.json | tr '\n' ' ')"
