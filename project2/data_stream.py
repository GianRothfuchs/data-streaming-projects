import logging
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as psf


# [DONE] Create a schema for incoming resources
schema = StructType([
    StructField("address", StringType(), True),
    StructField("address_type", StringType(), True),
    StructField("agency_id", StringType(), True),
    StructField("call_date", StringType(), True),
    StructField("call_date_time", StringType(), True),
    StructField("call_time", StringType(), True),
    StructField("city", StringType(), True),
    StructField("common_location", StringType(), True),
    StructField("crime_id", StringType(), True),
    StructField("disposition", StringType(), True),
    StructField("offense_date", StringType(), True),
    StructField("original_crime_type_name", StringType(), True),
    StructField("report_date", StringType(), True),
    StructField("state", StringType(), True)
])

radio_schema = StructType([
    StructField("disposition_code", StringType(), True),
    StructField("description", StringType(), True)
])

def run_spark_job(spark):

    # [DONE] Create Spark Configuration
    # Create Spark configurations with max offset of 200 per trigger
    # set up correct bootstrap server and port
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "com.udacity.spark.sf.policecalls") \
        .option("startingOffsets", "earliest") \
        .option("maxOffsetPerTrigger",200) \
        .option("stopGracefullyOnShutdown", "true") \
        .load()

    # Show schema for the incoming resources for checks
    df.printSchema()

    # [DONE] extract the correct column from the kafka input resources
    # Take only value and convert it to String
    kafka_df = df.selectExpr("CAST(value AS STRING)")

    service_table = kafka_df\
        .select(psf.from_json(psf.col('value'), schema).alias("DF"))\
        .select("DF.*")

    # [DONE] select original_crime_type_name and disposition
    distinct_table = service_table.select([
            psf.col('crime_id'),
            psf.col('original_crime_type_name'),
            psf.to_timestamp(psf.col('call_date_time')).alias('call_date_time'),
            psf.col('address'),
            psf.col('disposition')
    ])

    # count the number of original crime type
    agg_df = (
       distinct_table.withWatermark("call_date_time", "60 minutes")
       .groupBy(psf.col('original_crime_type_name')).count()
    )

    agg_df2 = distinct_table \
        .select(psf.col("original_crime_type_name"), psf.col("call_date_time"), psf.col("disposition"))\
        .withWatermark("call_date_time", "10 minutes") \
        .groupBy(psf.window(distinct_table.call_date_time, "10 minutes", "2 minutes"), distinct_table.original_crime_type_name, distinct_table.disposition) \
        .count()

    agg_df.printSchema()
    
    # [DONE] Q1. Submit a screen shot of a batch ingestion of the aggregation
    # [DONE] write output stream
    query = (
        agg_df.orderBy(psf.desc("count"))
        .writeStream
        .trigger(processingTime="60 seconds")
        .queryName("service_table")
        .outputMode('complete')
        .format('console')
        .option("truncate", "false")
        .start()
    )



    # [DONE] attach a ProgressReporter
    

    # [DONE] get the right radio code json path
    radio_code_json_filepath = "./radio_code.json"
    radio_code_df = (
                     spark
                     .read
                     .json(radio_code_json_filepath, schema=radio_schema, multiLine=True)
                     .withColumnRenamed("disposition_code", "disposition"))

    # clean up your data so that the column names match on radio_code_df and agg_df
    # we will want to join on the disposition code

    # DONE rename disposition_code column to disposition
    #radio_code_df = radio_code_df.
    

    
    # DONE join on disposition column

    join_query = agg_df2 \
        .join(radio_code_df, agg_df2.disposition == radio_code_df.disposition, 'inner') \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .start()
    
    query.awaitTermination()
    join_query.awaitTermination()


if __name__ == "__main__":
    logger = logging.getLogger(__name__)

    # TODO Create Spark in Standalone mode
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("KafkaSparkStructuredStreaming") \
        .config("spark.ui.port", 3000) \
        .getOrCreate()

    logger.info("Spark started")

    run_spark_job(spark)

    spark.stop()
