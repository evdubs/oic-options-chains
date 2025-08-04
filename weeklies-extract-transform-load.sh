#!/usr/bin/env bash

today=$(date "+%F")
dir=$(dirname "$0")
current_year=$(date "+%Y")

racket -y ${dir}/weeklies-extract.rkt
racket -y ${dir}/weeklies-transform-load.rkt -p "$1"

7zr a /var/local/oic/weeklies/${current_year}.7z /var/local/oic/weeklies/weeklyoptions.${today}.csv
