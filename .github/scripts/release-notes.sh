#!/usr/bin/env bash
# Prints the notes of one version's GitHub Release: the version's section of CHANGELOG.md,
# which its release pull request wrote, without its heading, then a link to every change
# since the version before. Prints nothing when the changelog has no section for the
# version, and the caller then keeps GitHub's generated notes.
#
# The changelog wraps its lines, and a release shows each line break, so each paragraph
# and list item is joined onto one line. Headings, blank lines, and code blocks stay.
#
# Usage: release-notes.sh CHANGELOG.md TAG PREVIOUS_TAG OWNER/REPO
# PREVIOUS_TAG may be empty, for the first version.
set -euo pipefail

changelog="$1"
tag="$2"
previous="$3"
repo="$4"

section=$(awk -v head="## ${tag} (" '
  index($0, head) == 1 { found = 1; next }
  found && /^## v/ { exit }
  found { print }
' "$changelog" | awk '
  function flush() { if (held != "") { print held; held = "" } }
  /^```/ { flush(); print; fenced = !fenced; next }
  fenced { print; next }
  /^[[:space:]]*$/ { flush(); print ""; next }
  /^#/ { flush(); print; next }
  /^([-*] |[0-9]+\. )/ { flush(); held = $0; next }
  { line = $0; sub(/^[[:space:]]+/, "", line); held = held == "" ? line : held " " line }
  END { flush() }
' | sed -e '/./,$!d')

if [ -z "${section//[[:space:]]/}" ]; then
  exit 0
fi
printf '%s\n' "$section"
if [ -n "$previous" ]; then
  printf '\n**Full Changelog**: https://github.com/%s/compare/%s...%s\n' "$repo" "$previous" "$tag"
fi
