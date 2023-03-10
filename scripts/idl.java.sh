#!/bin/sh -e
#
# Usage
# ./scripts/idl2schemata.java.sh tests/samples/simple.avdl
# output goes to out folder

mkdir -p out
docker run --rm --user="$(id -u)" \
    -v "$(pwd)":/avro kpnnv/avro-tools:1.11.1 idl "$@"
