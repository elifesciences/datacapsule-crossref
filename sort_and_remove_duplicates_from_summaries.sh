#!/bin/bash

set -e

source prepare-shell.sh

SUMMARIES_FILE=$DATA_PATH/crossref-works-summaries.tsv.gz
SUMMARIES_TEMP_FILE=$DATA_PATH/crossref-works-summaries.tsv.gz.temp
SUMMARIES_BACKUP_FILE=$DATA_PATH/crossref-works-summaries.tsv.gz.backup

if [ -f "$SUMMARIES_BACKUP_FILE" ]; then
  echo "Backup file already exists: $SUMMARIES_BACKUP_FILE (please confirm and delete or rename it)"
  exit 1
fi

mkdir -p "$TEMP_DIR"

# print the header (the first line of input)
# and then run the specified command on the body (the rest of the input)
# use it in a pipeline, e.g. ps | body grep somepattern
body() {
  IFS= read -r header
  printf '%s\n' "$header"
  "$@"
}

gunzip -c "$SUMMARIES_FILE" | pv | LC_ALL=C body sort -T "$TEMP_DIR" -u | gzip > "$SUMMARIES_TEMP_FILE"

mv "$SUMMARIES_FILE" "$SUMMARIES_BACKUP_FILE"
mv "$SUMMARIES_TEMP_FILE" "$SUMMARIES_FILE"
