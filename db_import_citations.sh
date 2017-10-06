#!/bin/bash

set -e

source prepare-shell.sh

run_sql() {
  PGPASSWORD=password psql -h localhost -p 8432 -U postgres -c "$@"
}

SOURCE_FILE="$DATA_PATH/crossref-works-citations.tsv.gz"

run_sql 'DROP TABLE IF EXISTS citations;'
run_sql 'CREATE TABLE citations(citing_doi TEXT NOT NULL, cited_doi TEXT NOT NULL);'
gunzip -c "$SOURCE_FILE" | tail -n +2 | sed 's:\\:\\\\:g' | run_sql 'COPY citations(citing_doi, cited_doi) FROM stdin;'
