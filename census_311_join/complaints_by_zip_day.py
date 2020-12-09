import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

header = ['zcta', 'date', 'type', 'descriptor', 'geoID', 'median_earning', 'full_time_median_earning', 'full_time_mean_earning']


def csv_to_df(spark):
    # https://stackoverflow.com/questions/47120778/add-column-names-to-data-read-from-csv-file-without-column-names
    return spark.read.format('csv').options(header='false').load(sys.argv[1]).toDF(*header)


def output(df, i):
    df.select("*").write.save(sys.argv[i], format='csv', emptyValue='null')


def main():
    spark = SparkSession.builder.appName('311-covid').getOrCreate()

    df = csv_to_df(spark)

    # group by zcta and day
    # https://stackoverflow.com/questions/1288058/conditional-count-on-a-field
    df.createOrReplaceTempView('complaints')
    df_zip_day = spark.sql("""
                      SELECT
                             zcta,
                             DATE_FORMAT(DATE_TRUNC('DD', TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a')), 'yyyy-MM-dd') AS day,
                             COUNT(*) AS num_complaints,
                             SUM(CASE WHEN LOWER(type) LIKE '%noise%' THEN 1 ELSE 0 END) AS num_noise_complaints,
                             FIRST(geoID),
                             FIRST(median_earning),
                             FIRST(full_time_median_earning),
                             FIRST(full_time_mean_earning)
                        FROM complaints
                       WHERE TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a') >= TO_DATE('03/01/2019', 'MM/dd/yyyy')
                         AND TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a') <  TO_DATE('11/01/2019', 'MM/dd/yyyy')
                          OR TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a') >= TO_DATE('03/01/2020', 'MM/dd/yyyy')
                         AND TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a') <  TO_DATE('11/01/2020', 'MM/dd/yyyy')
                    GROUP BY DATE_TRUNC('DD', TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a')), zcta
                    ORDER BY zcta, day
                   """)
    
    # group only by day
    df_zip_day.createOrReplaceTempView('grouped')
    df_day = spark.sql("""
                      SELECT
                             day,
                             SUM(num_complaints),
                             SUM(num_noise_complaints)
                        FROM grouped
                    GROUP BY day
                    ORDER BY day
                   """)

    output(df_zip_day, 2)
    output(df_day, 3)


if __name__ == '__main__':
    main()
