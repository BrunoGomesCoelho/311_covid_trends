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

    df = spark.sql("""SELECT `Created Date` AS date,
                             `Complaint Type` AS type,
                             Descriptor AS descriptor,
                             `Incident Zip` AS zip
                        FROM complaints
                       WHERE CHAR_LENGTH(`Complaint Type`) > 0
                         AND `Incident Zip` RLIKE '^1[0-9]{4}$'
                         AND TO_DATE(`Created Date`, 'MM/dd/yyyy hh:mm:ss a') >= TO_DATE('12/01/2010', 'MM/dd/yyyy')
                         AND TO_DATE(`Created Date`, 'MM/dd/yyyy hh:mm:ss a') < TO_DATE('11/01/2020', 'MM/dd/yyyy')""")

    output(df)


if __name__ == '__main__':
    main()
