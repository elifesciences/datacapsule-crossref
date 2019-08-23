#!/bin/bash

set -e

CSV_FILE="$1"

if [ -z "$CSV_FILE" ]; then
  echo "Usage: $0 <gzipped csv or tsv file>"
  exit 1
fi

CSV_FILE_BASENAME="${CSV_FILE##*/}"
CSV_FILE_EXT="${CSV_FILE_BASENAME##*.}"

if [ "$CSV_FILE_EXT" != "gz" ]; then
  echo "Expected .gz file extension (file: $CSV_FILE_BASENAME)"
  exit 2
fi

if [ ! -f "$CSV_FILE" ]; then
  echo "Input file does not exist: $CSV_FILE"
  exit 3
fi

CSV_TEMP_FILE="$CSV_FILE.temp"
CSV_BACKUP_FILE="$CSV_FILE.backup"

if [ -f "$CSV_BACKUP_FILE" ]; then
  echo "Backup file already exists: $CSV_BACKUP_FILE (please confirm and delete or rename it)"
  exit 4
fi

# print the header (the first line of input)
# and then run the specified command on the body (the rest of the input)
# use it in a pipeline, e.g. ps | body grep somepattern
body() {
  IFS= read -r header
  printf '%s\n' "$header"
  "$@"
}

gunzip -c "$CSV_FILE" | pv | LC_ALL=C body sort -T "$TEMP_DIR" -u | gzip > "$CSV_TEMP_FILE"

mv "$CSV_FILE" "$CSV_BACKUP_FILE"
mv "$CSV_TEMP_FILE" "$CSV_FILE"

echo "removed duplicates from ${CSV_FILE} (backup: ${CSV_BACKUP_FILE})"
echo "done"
