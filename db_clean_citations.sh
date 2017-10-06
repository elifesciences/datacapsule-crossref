#!/bin/bash

set -e

source prepare-shell.sh

run_sql() {
  PGPASSWORD=password psql -h localhost -p 8432 -U postgres -c "$@"
}

run_sql 'DROP TABLE IF EXISTS dois;'
run_sql 'CREATE TABLE dois(doi TEXT PRIMARY KEY);'
run_sql 'CREATE INDEX IF NOT EXISTS citations_citing_doi_index on citations (citing_doi);'
run_sql 'INSERT INTO dois(doi) SELECT distinct citing_doi FROM citations;'
