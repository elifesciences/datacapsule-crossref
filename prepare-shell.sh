#!/bin/bash

# Use this file by running:
# source prepare-shell.sh [--cloud]

set -e

DATA_PATH=data
TEMP_DIR=.temp

if [ -f .config ]; then
  source .config
fi
