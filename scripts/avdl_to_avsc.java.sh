#!/bin/sh -e
#
# Usage
# ./scripts/avdl_to_avsc.java.sh tests/samples/simple.avdl out

docker run --platform linux/amd64 --rm --user="$(id -u)" \
    -v "$(pwd)":/avro kpnnv/avro-tools:1.11.1 idl2schemata "$@"
