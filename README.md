# DataCapsule Crossref

Retrieve and extract citations from Crossref data.

The links to the latest dumps can be found in the [notebook](https://elifesci.org/crossref-data-notebook).

## Pre-requsites

* Python 3
* _pipeview_ (pv) to show progress in some shell scripts (ubuntu: `sudo apt-get install pv`)

## Setup

`pip install -r requirements.txt`

## Data Retrieval

Data is retrieved via the [Crossref's Works API](https://api.crossref.org/works) ([doc](https://github.com/CrossRef/rest-api-doc)).

Starting with the cursor _*_. The _data/crossref-works.zip.meta_ file contains the next cursor to use, should the download be interrupted for any reasons (it is likely it will). The download currently takes about 90 hours at the minimum and can't be run in parallel due to the way the cursor works.

To start or resume the download run:

```bash
./download_crossref_works.sh
```

The file _data/crossref-works.zip_ as well as _data/crossref-works.zip.meta_ will be created and updated. _crossref-works.zip_ will contain files with the raw response.

## Extract Citations

Run:

```bash
./extract_citations_from_crossref_works.sh [--multi-processing]
```

Note the `--multi-processing` flag is optional and may make the processing faster.

That will create _data/crossref-works-citations.tsv.gz_ a compressed tsv file with the following columns:

* _citing_doi_
* _cited_doi_

## Create Summary Stats

Run:

```bash
./extract_summaries_from_crossref_works.sh [--multi-processing] [--debug]
```

Note the `--multi-processing` flag is optional and may make the processing faster. The `--debug` flag is currently required as it will add another _debug_ column containing more details about references.

That will create _data/crossref-works-summaries.tsv.gz_ a compressed tsv file with a number of key features of works (used by the following step).

Run:

```bash
./citations_stats.sh
```

That will create the following files with summary stats:

* _data/crossref-works-summaries-stat.tsv_
* _data/crossref-works-summaries-by-type-and-publisher-stat.tsv.gz_
* _data/crossref-works-reference-stat.tsv.gz_
* _data/crossref-works-citations*.tsv_
