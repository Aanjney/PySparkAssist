#!/bin/sh
set -e
python -m pysparkassist.docker_bootstrap
exec "$@"
