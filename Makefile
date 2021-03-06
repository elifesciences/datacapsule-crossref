DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

USER_ID = $(shell id -u)
GROUP_ID = $(shell id -g)

RUN = $(DOCKER_COMPOSE) run --user $(USER_ID) --rm datacapsule-crossref
DEV_RUN = $(DOCKER_COMPOSE) run --rm datacapsule-crossref-dev

JUPYTER_DOCKER_COMPOSE = NB_UID="$(USER_ID)" NB_GID="$(GROUP_ID)" $(DOCKER_COMPOSE)
JUPYTER_RUN = $(JUPYTER_DOCKER_COMPOSE) run --rm jupyter

CROSSREF_WORKS_API_URL = https://api.crossref.org/works
ELIFE_CROSSREF_WORKS_API_URL = https://api.crossref.org/prefixes/10.7554/works

COMPRESSION = lzma
MAX_RETRIES =
EMAIL =

OUTPUT_SUFFIX =
FIGSHARE_PART_PREFIX = /tmp/part_

BQ_PROJECT =
BQ_DATASET =

SUMMARY_DATE = 2019-08-06

ARGS =


.PHONY: build


venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


dev-install:
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements.dev.txt


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 datacapsule_crossref tests


dev-pylint:
	$(PYTHON) -m pylint datacapsule_crossref tests


dev-lint: dev-flake8 dev-pylint


dev-pytest:
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS)


dev-watch:
	$(PYTHON) -m pytest_watch -- -p no:cacheprovider $(ARGS)


dev-test: dev-lint dev-pytest


build:
	$(DOCKER_COMPOSE) build datacapsule-crossref


build-dev:
	$(DOCKER_COMPOSE) build datacapsule-crossref-base-dev datacapsule-crossref-dev


flake8:
	$(DEV_RUN) flake8 datacapsule_crossref tests


pylint:
	$(DEV_RUN) pylint datacapsule_crossref tests


pytest:
	$(DEV_RUN) pytest -p no:cacheprovider $(ARGS)


lint: \
	flake8 \
	pylint


test: \
	lint \
	pytest


shell:
	$(RUN) bash


download-works:
	$(RUN) python -m datacapsule_crossref.download_works \
		--base-url=$(CROSSREF_WORKS_API_URL) \
		--compression=$(COMPRESSION) \
		--email=$(EMAIL) \
		--output-file=/data/crossref-works$(OUTPUT_SUFFIX).zip \
		$(ARGS)


download-works-elife:
	$(MAKE) \
		CROSSREF_WORKS_API_URL=$(ELIFE_CROSSREF_WORKS_API_URL) \
		OUTPUT_SUFFIX=-elife \
		download-works


extract-citations-from-works:
	$(RUN) python -m datacapsule_crossref.extract_citations_from_works \
		--input-file=/data/crossref-works$(OUTPUT_SUFFIX).zip \
		--output-file=/data/crossref-works$(OUTPUT_SUFFIX)-citations.tsv.gz \
		--multi-processing \
		$(ARGS)


extract-citations-from-works-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife extract-citations-from-works


extract-summaries-from-works:
	$(RUN) python -m datacapsule_crossref.extract_summaries_from_works \
		--input-file=/data/crossref-works$(OUTPUT_SUFFIX).zip \
		--output-file=/data/crossref-works$(OUTPUT_SUFFIX)-summaries.tsv.gz \
		--multi-processing \
		--debug
		$(ARGS)


extract-summaries-from-works-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife extract-summaries-from-works


sort-and-remove-duplicates-from-citations:
	$(RUN) sort-and-remove-duplicates-from-csv.sh /data/crossref-works$(OUTPUT_SUFFIX)-citations.tsv.gz


sort-and-remove-duplicates-from-citations-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife sort-and-remove-duplicates-from-citations


sort-and-remove-duplicates-from-summaries:
	$(RUN) sort-and-remove-duplicates-from-csv.sh /data/crossref-works$(OUTPUT_SUFFIX)-summaries.tsv.gz


sort-and-remove-duplicates-from-summaries-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife sort-and-remove-duplicates-from-summaries


generate-csv-stats:
	$(RUN) csv-stats.sh \
		/data/crossref-works$(OUTPUT_SUFFIX)-summaries.tsv.gz \
		/data/crossref-works$(OUTPUT_SUFFIX)-summaries-stat.tsv.gz


generate-csv-stats-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife generate-csv-stats


generate-csv-stats-grouped-by-type-and-publisher:
	$(RUN) csv-stats.sh \
		/data/crossref-works$(OUTPUT_SUFFIX)-summaries.tsv.gz \
		/data/crossref-works$(OUTPUT_SUFFIX)-summaries-by-type-and-publisher-stat.tsv.gz \
		--group-by=type,publisher


generate-csv-stats-grouped-by-type-and-publisher-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife generate-csv-stats-grouped-by-type-and-publisher


generate-reference-stats:
	$(RUN) reference-stats.sh \
		/data/crossref-works$(OUTPUT_SUFFIX)-summaries.tsv.gz \
		/data/crossref-works$(OUTPUT_SUFFIX)-reference-stat.tsv.gz


generate-reference-stats-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife generate-reference-stats


.require-BQ_PROJECT:
	@if [ -z "$(BQ_PROJECT)" ]; then \
		echo "BQ_PROJECT required"; \
		exit 1; \
	fi


.require-BQ_DATASET:
	@if [ -z "$(BQ_DATASET)" ]; then \
		echo "BQ_DATASET required"; \
		exit 1; \
	fi


bq-upload-citations: .require-BQ_PROJECT .require-BQ_DATASET
	bq --project "$(BQ_PROJECT)" \
		load --autodetect --replace \
		--source_format=CSV \
		--format=csv \
		--skip_leading_rows=1 \
		--quote='' \
		--field_delimiter=tab \
		"$(BQ_DATASET).citation_links" \
		"./data/crossref-works$(OUTPUT_SUFFIX)-citations.tsv.gz" \
		citing_doi,cited_doi


bq-upload-citations-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife bq-upload-citations


bq-upload-summaries: .require-BQ_PROJECT .require-BQ_DATASET
	bq --project "$(BQ_PROJECT)" \
		load --autodetect --replace \
		--source_format=CSV \
		--format=csv \
		--skip_leading_rows=1 \
		--quote='' \
		--field_delimiter=tab \
		"$(BQ_DATASET).work_summary" \
		"./data/crossref-works$(OUTPUT_SUFFIX)-summaries.tsv.gz" \
		doi,title,reference_count:INT64,referenced_by_count:INT64,created:TIMESTAMP,type,publisher,container_title,author_count:INT64,first_subject_area,subject_areas,has_references:BOOL,num_references:INT64,num_citations_without_doi:INT64,num_duplicate_citation_dois:INT64,cited_dois,references


bq-upload-summaries-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife bq-upload-summaries


figshare-upload-works:
	$(RUN) figshare-upload.sh \
		/data/crossref-works$(OUTPUT_SUFFIX).zip "" "$(FIGSHARE_PART_PREFIX)"


figshare-upload-works-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife figshare-upload-works


figshare-upload-citations:
	$(RUN) figshare-upload.sh \
		/data/crossref-works$(OUTPUT_SUFFIX)-citations.tsv.gz "" "$(FIGSHARE_PART_PREFIX)"


figshare-upload-citations-elife:
	$(MAKE) OUTPUT_SUFFIX=-elife figshare-upload-citations


jupyter-build:
	@if [ "$(NO_BUILD)" != "y" ]; then \
		$(JUPYTER_DOCKER_COMPOSE) build jupyter; \
	fi


jupyter-shell: jupyter-build
	$(JUPYTER_RUN) bash


jupyter-start: jupyter-build
	$(JUPYTER_DOCKER_COMPOSE) up -d jupyter


jupyter-logs:
	$(JUPYTER_DOCKER_COMPOSE) logs -f jupyter


jupyter-stop:
	$(JUPYTER_DOCKER_COMPOSE) down


download-summary-stats:
	curl --output ./data/crossref-works-summaries-stat.tsv.gz \
		https://storage.googleapis.com/elife-ml/citations/by-date/$(SUMMARY_DATE)/crossref-works-summaries-stat.tsv.gz
	curl --output ./data/crossref-works-summaries-by-type-and-publisher-stat.tsv.gz \
		https://storage.googleapis.com/elife-ml/citations/by-date/$(SUMMARY_DATE)/crossref-works-summaries-by-type-and-publisher-stat.tsv.gz
	curl --output ./data/crossref-works-reference-stat.tsv.gz \
		https://storage.googleapis.com/elife-ml/citations/by-date/$(SUMMARY_DATE)/crossref-works-reference-stat.tsv.gz
	ls -lh ./data


ci-build-and-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build build-dev test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
