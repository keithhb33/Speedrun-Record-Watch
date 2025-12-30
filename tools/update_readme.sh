#!/usr/bin/env bash
set -euo pipefail

README="README.md"
NEW_CONTENT_FILE="${1:-}"

if [[ -z "${NEW_CONTENT_FILE}" || ! -f "${NEW_CONTENT_FILE}" ]]; then
  echo "Usage: tools/update_readme.sh <generated_markdown_file>" >&2
  exit 2
fi

START="<!-- WR-WEEKLY:START -->"
END="<!-- WR-WEEKLY:END -->"

tmp="$(mktemp)"

awk -v start="$START" -v end="$END" -v newfile="$NEW_CONTENT_FILE" '
BEGIN { inblock=0 }
{
  if ($0 == start) {
    print $0
    while ((getline line < newfile) > 0) print line
    close(newfile)
    inblock=1
    next
  }
  if ($0 == end) {
    inblock=0
    print $0
    next
  }
  if (!inblock) print $0
}
' "$README" > "$tmp"

mv "$tmp" "$README"
