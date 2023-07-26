#!/bin/bash

while read INPUT; do
  echo $INPUT | jq .data
done