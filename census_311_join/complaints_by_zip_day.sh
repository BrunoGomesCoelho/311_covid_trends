#!/bin/bash
if [ $# -eq 0 ]
	then
		OUTPUT_PATH_ZIP_DAY='/user/djk525/big-data/project/data/complaints_census_by_zip_day.csv'
		OUTPUT_PATH_DAY='/user/djk525/big-data/project/data/complaints_by_day.csv'
		LOCAL_PATH_ZIP_DAY="complaints_census_by_zip_day.csv"
		LOCAL_PATH_DAY="complaints_by_day.csv"
	else
		OUTPUT_PATH_ZIP_DAY=$1
		OUTPUT_PATH_DAY=$2
		LOCAL_PATH_ZIP_DAY="complaints_census_by_zip_day_cmp.csv"
		LOCAL_PATH_DAY="complaints_by_day_cmp.csv"
fi

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    complaints_by_zip_day.py \
    /user/jr4964/final-project/complaint_census_join.csv \
    $OUTPUT_PATH_ZIP_DAY \
    $OUTPUT_PATH_DAY

/usr/bin/hadoop fs -getmerge $OUTPUT_PATH_ZIP_DAY $LOCAL_PATH_ZIP_DAY
/usr/bin/hadoop fs -getmerge $OUTPUT_PATH_DAY $LOCAL_PATH_DAY

# add header
sed -i '1s/^/zcta,day,num_complaints,num_noise_complaints,geoID,median_earning,full_time_median_earning,full_time_mean_earning\n/' $LOCAL_PATH_ZIP_DAY
sed -i '1s/^/day,num_complaints,num_noise_complaints\n/' $LOCAL_PATH_DAY

# remove .crc file to prevent checksum error when putting file back to hfs
rm "."$LOCAL_PATH_ZIP_DAY.crc
rm "."$LOCAL_PATH_DAY.crc

/usr/bin/hadoop fs -rm -r $OUTPUT_PATH_ZIP_DAY
/usr/bin/hadoop fs -rm -r $OUTPUT_PATH_DAY

/usr/bin/hadoop fs -put $LOCAL_PATH_ZIP_DAY $OUTPUT_PATH_ZIP_DAY
/usr/bin/hadoop fs -put $LOCAL_PATH_DAY $OUTPUT_PATH_DAY
