#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR" || exit 1

echo "Starting update in directory: $SCRIPT_DIR"
echo "Pulling from origin main..."

OUTPUT=$(git pull origin main)
STATUS=$?

if [ $STATUS -ne 0 ]; then
  echo "Error during git pull!"
  read -rp "Press Enter to exit..."
  exit $STATUS
fi

if echo "$OUTPUT" | grep -q "Already up to date."; then
  echo "No changes found. Repository is up to date."
else
  echo "Update completed:"
  echo "$OUTPUT"
  echo "Latest commit(s):"
  git log -3 --oneline
fi

read -rp "Press Enter to exit..."
