#!/bin/bash

set -e

source prepare-shell.sh

./scripts/sort-and-remove-duplicates-from-csv.sh $DATA_PATH/crossref-works-citations.tsv.gz
