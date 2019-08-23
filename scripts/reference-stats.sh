#!/bin/bash

set -e

CSV_FILE="$1"
OUTPUT_FILE="$2"

if [ -z "$CSV_FILE" ] || [ -z "$OUTPUT_FILE" ]; then
  echo "Usage: $0 <gzipped csv or tsv file> <output file> [<args>]"
  exit 1
fi

shift 2

pv -f "${CSV_FILE}" | \
  zcat - | \
  python -m datacapsule_crossref.reference_stats $@ | \
  gzip > "${OUTPUT_FILE}"

echo "created reference stats ${OUTPUT_FILE} (source: ${CSV_FILE})"
echo "done"
