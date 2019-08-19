DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = $(VENV)/bin/python

RUN = $(DOCKER_COMPOSE) run --rm datacapsule-crossref
DEV_RUN = $(DOCKER_COMPOSE) run --rm datacapsule-crossref-dev


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


ci-build-and-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build build-dev test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
