DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

RUN = $(DOCKER_COMPOSE) run --rm datacapsule-crossref
DEV_RUN = $(DOCKER_COMPOSE) run --rm datacapsule-crossref-dev


ARGS =


.PHONY: build


build:
	$(DOCKER_COMPOSE) build datacapsule-crossref


build-dev:
	$(DOCKER_COMPOSE) build datacapsule-crossref-base-dev datacapsule-crossref-dev


flake8:
	$(DEV_RUN) flake8 datacapsule_crossref


pylint:
	$(DEV_RUN) pylint datacapsule_crossref


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
