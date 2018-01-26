#!/bin/bash

set -e

source prepare-shell.sh

SUMMARY_FILE=$DATA_PATH/crossref-works-summaries.tsv.gz
SUMMARY_STAT_FILE=$DATA_PATH/crossref-works-summaries-stat.tsv
SUMMARY_BY_PUBLISHER_STAT_FILE=$DATA_PATH/crossref-works-summaries-by-type-and-publisher-stat.tsv.gz
REFERENCE_STAT_FILE=$DATA_PATH/crossref-works-reference-stat.tsv.gz

echo "generate summary stats"
pv "$SUMMARY_FILE" | zcat - | \
  tee >(python -m datacapsule_crossref.csv_stats --header --group-by=type,publisher | gzip > $SUMMARY_BY_PUBLISHER_STAT_FILE) | \
  tee >(python -m datacapsule_crossref.reference_stats | gzip > $REFERENCE_STAT_FILE) | \
  python -m datacapsule_crossref.csv_stats --header \
  > "$SUMMARY_STAT_FILE"

cat "$SUMMARY_STAT_FILE"
