#!/bin/bash

# Note: these stats are frequently updated and require to run process of cleaning the dois (separate branch)

set -e

source prepare-shell.sh

CLEANED_CITATIONS_FILE=$DATA_PATH/crossref-works-citations-cleaned.tsv.gz
CLEANED_CITATIONS_STAT_FILE=$DATA_PATH/crossref-works-citations-stat.tsv
CORRECTED_NON_CASE_INSENSITIVE_CITATIONS_STAT_FILE=$DATA_PATH/crossref-works-citations-corrected-non-case-insensitive.tsv
UNMATCHED_CITATIONS_STAT_FILE=$DATA_PATH/crossref-works-citations-unmatched.tsv


add_doi_corrected_equal_case_insensitve_column() {
  # if a there is a cited_doi ($2) and it is marked as corrected ($5):
  #   add a boolean value whether cited_doi ($2) equals original_cited_doi ($3) ignoring case
  # otherwise:
  #   add an empty column 
  awk -F$'\t' \
  '(NR==1){print $0, "doi_corrected_equal_case_insensitve";}
  (NR>1) && ($2!="") && ($5=="true") {print $0, tolower($2)==tolower($3)};
  (NR>1) && (($2=="") || ($5!="true")) {print $0, ""}' OFS=$'\t'  
}

filter_doi_corrected_not_equal_case_insenstive() {
  # accept if there is a cited_doi ($2) and it is marked corrected but not equal case insensitve ($6)
  awk -F$'\t' \
  '(NR==1){print $0}
  (NR>1) && ($2!="") && ($6=="false") {print $0}' OFS=$'\t'  
}

filter_unmatched_dois() {
  # accept if there is an original_cited_doi ($3) and it is marked as not valid ($4)
  awk -F$'\t' \
  '(NR==1){print $0}
  (NR>1) && ($3!="") && ($4=="false") {print $0}' OFS=$'\t'
}

echo "generate citations stats"
pv "$CLEANED_CITATIONS_FILE" | zcat - | \
  tr -d '\r' |
  add_doi_corrected_equal_case_insensitve_column | \
  tee >(filter_doi_corrected_not_equal_case_insenstive > $CORRECTED_NON_CASE_INSENSITIVE_CITATIONS_STAT_FILE) | \
  tee >(filter_unmatched_dois > $UNMATCHED_CITATIONS_STAT_FILE) | \
  python -m datacapsule_crossref.csv_stats --header \
  > "$CLEANED_CITATIONS_STAT_FILE"

cat "$CLEANED_CITATIONS_STAT_FILE"
