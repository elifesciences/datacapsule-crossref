#!/bin/bash

set -e

source prepare-shell.sh

RUN_ARGS=(
  --output-file $DATA_PATH/crossref-works.zip
  $@
)

python -m datacapsule_crossref.download_works ${RUN_ARGS[@]}
