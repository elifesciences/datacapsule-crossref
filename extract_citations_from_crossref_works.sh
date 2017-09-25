#!/bin/bash

set -e

source prepare-shell.sh

RUN_ARGS=(
  --input-file $DATA_PATH/crossref-works.zip
  --output-file $DATA_PATH/crossref-works-citations.csv.gz
  $@
)

python -m datacapsule_crossref.extract_citations_from_works ${RUN_ARGS[@]}
