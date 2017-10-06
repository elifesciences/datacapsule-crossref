#!/bin/bash

set -e

source prepare-shell.sh

BASE_CITATIONS_NAME=$DATA_PATH/crossref-works-citations
LMDB_ROOT="$BASE_CITATIONS_NAME-lmdb"

if [ ! -d "$LMDB_ROOT" ]; then
  RUN_ARGS=(
    --input-file $BASE_CITATIONS_NAME.tsv.gz
    --output-root "$LMDB_ROOT"
    $@
  )

  python -m datacapsule_crossref.csv_to_lmdb ${RUN_ARGS[@]}
fi

RUN_ARGS=(
  --input-file $BASE_CITATIONS_NAME.tsv.gz
  --lmdb-root "$LMDB_ROOT"
  --output-file $DATA_PATH/crossref-works-citations-cleaned.tsv.gz
  $@
)

python -m datacapsule_crossref.clean_and_extract_citations_from_lmdb ${RUN_ARGS[@]}
