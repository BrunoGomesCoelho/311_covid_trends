#!/bin/bash
# Used in the pipeline_compare scripts
if [ $# -eq 0 ]
	then
		OUTPUT_PATH="/user/djk525/big-data/project/data/311_reduced.csv"
		LOCAL_PATH="311_sample_reduced.csv"

	else
		OUTPUT_PATH=$1
		LOCAL_PATH="311_sample_reduced_pipeline_cmp.csv"
fi

module load python/gnu/3.6.5
module load spark/2.4.0

time spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    --conf spark.ui.port=4050 \
    prepare_311_data.py \
    /user/jr4964/final-project/sample_complaints.csv \
    /user/djk525/big-data/project/data/zip_zcta.csv \
    $OUTPUT_PATH 

/usr/bin/hadoop fs -getmerge $OUTPUT_PATH $LOCAL_PATH

# add header
sed -i '1s/^/date,type,descriptor,zcta\n/' 311_sample_reduced.csv

# remove .crc file to prevent checksum error when putting file back to hfs
rm "."$LOCAL_PATH".crc"

/usr/bin/hadoop fs -rm -r $OUTPUT_PATH
/usr/bin/hadoop fs -put $LOCAL_PATH $OUTPUT_PATH

