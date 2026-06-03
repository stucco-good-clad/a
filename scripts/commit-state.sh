#!/usr/bin/env bash
set -euo pipefail

: "${END:?END is required}"

git config --local user.name "backfill-bot"
git config --local user.email "actions@github.com"
git stash --include-untracked
git pull --rebase
git stash pop || true
git add state.json
if git diff --cached --quiet; then
  echo "No changes to state.json, skipping commit"
else
  git commit -m "backfill: advance to ${END}"
  git push
fi
