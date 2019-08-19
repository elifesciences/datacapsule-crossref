DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)


.PHONY: build


build:
	$(DOCKER_COMPOSE) build datacapsule-crossref


ci-build-and-test:
	$(MAKE) DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" \
		build


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
