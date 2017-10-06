#!/bin/bash

set -e

source prepare-shell.sh

PG_DATA=`realpath "$TEMP_DIR/pgdata"`
CONTAINER_PGDATA=/var/lib/postgresql/data

mkdir -p "$PG_DATA"

docker rm -f citations-postgres
docker run --name citations-postgres -p 8432:5432 -e POSTGRES_PASSWORD=password -e PGDATA=$CONTAINER_PGDATA -v $PG_DATA:$CONTAINER_PGDATA -d postgres
