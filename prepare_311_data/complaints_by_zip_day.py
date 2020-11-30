import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def csv_to_df(spark):
    return spark.read.format('csv').options(header='true').load(sys.argv[1])


def output(df):
    df.select("*").write.save(sys.argv[2], format='csv', emptyValue='null')


def main():
    spark = SparkSession.builder.appName('311-covid').getOrCreate()

    df = csv_to_df(spark)

    df.createOrReplaceTempView('complaints')

    df = spark.sql("""SELECT DATE_TRUNC('DD', TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a')) as day,
                             zip,
                             COUNT(*) as num_complaints
                        FROM complaints
                    GROUP BY DATE_TRUNC('DD', TO_DATE(date, 'MM/dd/yyyy hh:mm:ss a')), zip
                    ORDER BY day, zip
                   """)

    output(df)


if __name__ == '__main__':
    main()
