#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")

racket -y ${dir}/extract.rkt -p "$1"
racket -y ${dir}/transform-load.2022-04-29.rkt -p "$1"

7zr a /var/tmp/oic/options-chains/${today}.7z /var/tmp/oic/options-chains/${today}/*.html

racket -y ${dir}/dump-dolt.rkt -p "$1"
