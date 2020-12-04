

module load python/gnu/3.6.5
module load spark/2.4.0

spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python complaints_census_join.py /user/jr4964/final-project/census_data.csv /user/djk525/big-data/project/data/311_reduced.csv
