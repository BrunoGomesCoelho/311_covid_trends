import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

def csv_to_df(spark, i):
    return spark.read.format('csv').options(header='true').load(sys.argv[i])


def output(df):
    df.select("*").write.save(sys.argv[3], format='csv', emptyValue='null')


def main():
    spark = SparkSession.builder.appName('311-covid').getOrCreate()

    df = csv_to_df(spark, 1)
    zips = csv_to_df(spark, 2)

    df.createOrReplaceTempView('complaints')
    zips.createOrReplaceTempView('zips')

    df = spark.sql("""SELECT `Created Date` AS date,
                             `Complaint Type` AS type,
                             Descriptor AS descriptor,
                             zcta
                        FROM complaints c
                  INNER JOIN zips z
                          ON c.`Incident Zip` = z.zip
                       WHERE CHAR_LENGTH(`Complaint Type`) > 0
                         AND TO_DATE(`Created Date`, 'MM/dd/yyyy hh:mm:ss a') >= TO_DATE('12/01/2010', 'MM/dd/yyyy')
                         AND TO_DATE(`Created Date`, 'MM/dd/yyyy hh:mm:ss a') < TO_DATE('11/01/2020', 'MM/dd/yyyy')""")

    output(df)


if __name__ == '__main__':
    main()
