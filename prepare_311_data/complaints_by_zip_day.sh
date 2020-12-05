#!/bin/bash
if [ $# -eq 0 ]
	then
		OUTPUT_PATH='/user/djk525/big-data/project/data/complaints_by_zip_day.csv'
		LOCAL_PATH="complaints_by_zip_day.csv"
	else
		OUTPUT_PATH=$1
		LOCAL_PATH="complaints_by_zip_day_cmp.csv"
fi

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    complaints_by_zip_day.py \
    /user/djk525/big-data/project/data/311_reduced.csv \
    $OUTPUT_PATH

/usr/bin/hadoop fs -getmerge $OUTPUT_PATH $LOCAL_PATH

# add header
sed -i '1s/^/date,zcta,num_complaints\n/' $LOCAL_PATH

# remove .crc file to prevent checksum error when putting file back to hfs
rm "."$LOCAL_PATH.crc


/usr/bin/hadoop fs -rm -r $OUTPUT_PATH
/usr/bin/hadoop fs -put $LOCAL_PATH $OUTPUT_PATH
