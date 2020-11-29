#!/bin/bash

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    prepare_311_data.py \
    /user/jr4964/final-project/complaints.csv \
    /user/djk525/big-data/project/data/311_reduced.csv \

/usr/bin/hadoop fs -getmerge big-data/project/data/311_reduced.csv 311_reduced.csv
/usr/bin/hadoop fs -rm -r big-data/project/data/311_reduced.csv
/usr/bin/hadoop fs -put 311_reduced.csv big-data/project/data/311_reduced.csv
