#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")

racket -y ${dir}/extract.2023-11-16.rkt -p "$1" -e "$2" -s "$3"
racket -y ${dir}/transform-load.2023-11-16.rkt -p "$1"

7zr a /var/tmp/oic/options-chains/${today}.7z /var/tmp/oic/options-chains/${today}/*.html /var/tmp/oic/options-chains/${today}/*.json

racket -y ${dir}/dump-dolt.rkt -p "$1"
