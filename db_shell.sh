#!/bin/bash

set -e

PGPASSWORD=password psql -h localhost -p 8432 -U postgres