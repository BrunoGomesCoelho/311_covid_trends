# Used in the pipeline_compare scripts
if [ $# -eq 0 ]
	then
		OUTPUT_PATH='/user/jr4964/final-project/complaint_census_join.out'
	else
		OUTPUT_PATH=$1
fi

module load python/gnu/3.6.5
module load spark/2.4.0

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python complaints_census_join.py /user/jr4964/final-project/census_data.csv /user/djk525/big-data/project/data/311_reduced.csv $OUTPUT_PATH

