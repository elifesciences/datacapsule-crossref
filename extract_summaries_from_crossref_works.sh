#!/bin/bash

set -e

source prepare-shell.sh

RUN_ARGS=(
  --input-file $DATA_PATH/crossref-works.zip
#  --output-file $DATA_PATH/crossref-works-summaries.tsv.gz
  --output-file /dev/stdout
  $@
)

OUTPUT_FILE=$DATA_PATH/crossref-works-summaries.tsv.gz


# print the header (the first line of input)
# and then run the specified command on the body (the rest of the input)
# use it in a pipeline, e.g. ps | body grep somepattern
body() {
  IFS= read -r header
  printf '%s\n' "$header"
  "$@"
}

# python -m datacapsule_crossref.extract_summaries_from_works ${RUN_ARGS[@]} --output-file="$OUTPUT_FILE"

python -m datacapsule_crossref.extract_summaries_from_works ${RUN_ARGS[@]} --delimiter=$'\t' | \
  gzip > "$OUTPUT_FILE"

  # LC_ALL=C body sort --buffer-size=1M |