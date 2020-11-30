#!/bin/bash

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    prepare_311_data.py \
    /user/jr4964/final-project/sample_complaints.csv \
    /user/djk525/big-data/project/data/311_sample_reduced.csv \

/usr/bin/hadoop fs -getmerge big-data/project/data/311_sample_reduced.csv 311_sample_reduced.csv

# add header
sed -i '1s/^/date,type,descriptor,zip\n/' 311_sample_reduced.csv
rm .311_sample_reduced.csv.crc 

/usr/bin/hadoop fs -rm -r big-data/project/data/311_sample_reduced.csv
/usr/bin/hadoop fs -put 311_sample_reduced.csv big-data/project/data/311_sample_reduced.csv
