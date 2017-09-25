Retrieve and extract citations from Crossref data.

# Pre-requsites

* Python 2 or 3 (Python 3 preferred)

# Setup

`pip install -r requirements.txt`

# Data Retrieval

Data is retrieved via the [Crossref's Works API](https://api.crossref.org/works) ([doc](https://github.com/CrossRef/rest-api-doc)).

Starting with the cursor _*_. The _data/crossref-works.zip.meta_ file contains the next cursor to use, should the download be interrupted for any reasons (it is likely it will). The download currently takes about 90 hours at the minimum and can't be run in parallel due to the way the cursor works.

To start or resume the download run:

```bash
./download_crossref_works.sh
```

The file _data/crossref-works.zip_ as well as _data/crossref-works.zip.meta_ will be created and updated. _crossref-works.zip_ will contain files with the raw response.

# Extract Citations

Run:

```bash
./extract_citations_from_crossref_works.sh
```

That will create _data/crossref-works-citations.csv.gz_ a compressed csv file with the following columns:

* _citing_doi_
* _cited_doi_

