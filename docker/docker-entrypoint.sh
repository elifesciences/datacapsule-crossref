#!/bin/bash

set -e

# workaround for permissions issue (to allow notebooks to be updated and added from the container)

chmod -R a+w .

ls -la

exec "$@"
