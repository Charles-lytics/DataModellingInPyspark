import os
from distutils.command.config import config

from pyspark.sql import *
from pyspark.sql.functions import spark_partition_id, month, monotonically_increasing_id
from pyspark.sql.functions import current_date, year

from pyspark.sql.types import IntegerType, StructType, StructField, StringType, DateType

from datetime import datetime, date

from pyspark import SparkContext

from lib.logger import Log4j

# Building my spark session
if __name__ == "__main__":
    os.environ['JAVA_HOME'] = "C:\java\jdk-11.0.23+9"
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("MyFirstDB") \
        .enableHiveSupport() \
        .getOrCreate()

    logger = Log4j(spark)

    # Loading my data
    patientDataDF = spark.read \
        .format("csv") \
        .load("datasource/data.csv")
    # .option("header", "true") \
    # .option("inferSchema", "true") \
    # Showing my data
    # patientDataDF.show()

# Constructing my Schema
    patientDataSchema = StructType([
        StructField("Name", StringType(), True),
        StructField("Gender", StringType(), True),
        StructField("Profession", StringType(), True),
        StructField("State", StringType(), True),
        StructField("asOfDate", DateType(), True),
        StructField("Temperature", IntegerType(), True),
        StructField("Pulse", IntegerType(), True)
    ])

# Loading my data

    patientDataDF = spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load("datasource/data.csv")

    patientDataDF = patientDataDF.withColumn("UniqueID", monotonically_increasing_id)
    patientDataDF = patientDataDF.withColumn('year', year(patientDataDF.asOfDate))
    patientDataDF = patientDataDF.withColumn('month', month(patientDataDF.asOfDate))


