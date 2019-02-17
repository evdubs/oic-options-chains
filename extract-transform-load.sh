#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")

racket ${dir}/extract.rkt -p "$1"
racket ${dir}/transform-load.rkt -p "$1"

7zr a /var/tmp/oic/options-chains/${today}.7z /var/tmp/oic/options-chains/${today}/*.html
