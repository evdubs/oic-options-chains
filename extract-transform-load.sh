#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")

racket ${dir}/extract.rkt -j "$1" -p "$2"
racket ${dir}/transform-load.rkt -p "$2"

7zr a /var/tmp/oic/options-chains/${today}.7z /var/tmp/oic/options-chains/${today}/*.html
