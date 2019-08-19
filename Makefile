DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

RUN = $(DOCKER_COMPOSE) run --rm datacapsule-crossref
DEV_RUN = $(RUN)


ARGS =


.PHONY: build


build:
	$(DOCKER_COMPOSE) build datacapsule-crossref


pytest:
	$(DEV_RUN) pytest -p no:cacheprovider $(ARGS)


test: \
	pytest


ci-build-and-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
