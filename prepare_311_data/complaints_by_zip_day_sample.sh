#!/bin/bash

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    complaints_by_zip_day.py \
    /user/djk525/big-data/project/data/311_sample_reduced.csv \
    /user/djk525/big-data/project/data/complaints_by_zip_day_sample.csv \

/usr/bin/hadoop fs -getmerge big-data/project/data/complaints_by_zip_day_sample.csv complaints_by_zip_day_sample.csv

# add header
sed -i '1s/^/date,zip,num_complaints\n/' complaints_by_zip_day_sample.csv

# remove .crc file to prevent checksum error when putting file back to hfs
rm .complaints_by_zip_day_sample.csv.crc

/usr/bin/hadoop fs -rm -r big-data/project/data/complaints_by_zip_day_sample.csv
/usr/bin/hadoop fs -put complaints_by_zip_day_sample.csv big-data/project/data/complaints_by_zip_day_sample.csv
