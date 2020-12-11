# Used in the pipeline_compare scripts
if [ $# -eq 0 ]
	then
		OUTPUT_PATH='/user/djk525/big-data/project/data/complaint_census_join.csv'
		LOCAL_PATH='complaint_census_join.csv'
	else
		OUTPUT_PATH=$1
		LOCAL_PATH='complaint_census_join_cmp.csv'
fi

module load python/gnu/3.6.5
module load spark/2.4.0

time spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python \
    complaints_census_join.py \
    /user/jr4964/final-project/census_data.csv \
    /user/djk525/big-data/project/data/311_reduced.csv \
    $OUTPUT_PATH

/usr/bin/hadoop fs -getmerge $OUTPUT_PATH $LOCAL_PATH

/usr/bin/hadoop fs -rm -r $OUTPUT_PATH

/usr/bin/hadoop fs -put $LOCAL_PATH $OUTPUT_PATH
