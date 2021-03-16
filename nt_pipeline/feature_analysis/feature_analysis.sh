#!/bin/bash

day=$1

aws s3 sync s3://sprs.push.us-east-1.prod/data/warehouse/model/model_analysis/$day data/$day
cat data/$day/* > data/$day.feature
rm -r data/$day

python feature_analysis.py data/$day.feature | sort -k 4 -g -r > mi_result.$day
