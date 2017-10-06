#!/bin/bash

set -e

source prepare-shell.sh

CITATIONS_FILE=$DATA_PATH/crossref-works-citations.tsv.gz
NUMBERED_DOIS_FILE=$DATA_PATH/crossref-works-dois.tsv.gz

mkdir -p "$TEMP_DIR"

remove_header() { tail -n +2; }
flatten_dois() { tr -d '\r' | tr '\t' '\n'; } # flatten dois to one list of dois
sort_drop_duplicates() { LC_ALL=C sort -T "$TEMP_DIR" -u; } # may not completely drop duplicates?
add_line_number_as_id() { awk '{ print (NR - 1) "\t" $0 }'; }
add_header() { { printf 'id\tdoi\n'; cat -; }; }

gunzip -c "$CITATIONS_FILE" | pv | \
  remove_header | flatten_dois | sort_drop_duplicates | uniq | add_line_number_as_id | add_header | \
  gzip > "$NUMBERED_DOIS_FILE"
