# Basic imports
import os, sys, time, json

import pyspark
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T

# Create a Spark session
# To configurate the connection between Apache Kafka and Pyspark, it is necessary to run four jar files

def spark_initialize() -> SparkSession:
    scala_version = '2.12'
    spark_version = '3.3.1'
    packages = [f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
                f'org.apache.spark:spark-token-provider-kafka-0-10_{scala_version}:{spark_version}',
                f'org.apache.spark:spark-streaming-kafka-0-10_{scala_version}:{spark_version}',
                'org.apache.kafka:kafka-clients:3.3.1',
                'org.apache.commons:commons-pool2:2.8.0'
            ]
    spark = SparkSession.builder\
        .appName('Streaming')\
        .config('spark.jars.packages', ','.join(packages))\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # INFO, WARN, ERROR
    return spark

def data_stream_spark(spark, brokers, topic, table) -> DataFrame:

    # Setup a streaming DataFrame to read data from kafka consumer
    # Subscribe to 1 topic, with headers
    df = ( spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("includeHeaders", "true")
        .option("startingOffsets", "earliest") # earliest latest
        .load()
        )

    # Just in case we want to start a table containing results but from scratch
    spark.sql(f'drop table if exists {table}')

    # Setup a streaming query
    # In this case we want to store in an in-memory table (the sink).
    # The query name will be the table name
    # After executing the code, the streaming computation will start in the backgro
    query = ( df
        .writeStream
        .queryName(f'{table}')
        .outputMode("append") # append, update
        .format("memory")
        .start()
    )
    return query

# Notice that in a production environment, we have to establish
# that the query is awaiting termination so to prevent the driver
# process from termination when the stream is ative

def await_termination(query):
    query.awaitTermination()

# Stopping the query process
def stop(query):
    query.stop()

# Show the status of the query
def show_status(spark, query):
    print(f'Active: {spark.streams.active[0].isActive}.')
    print(f'Status of query: {query.status}.')

# Figure out the tables we hold
def show_tables(spark):
    spark.sql("show tables").show(truncate=False)

# Check all the info stored, in the sink/table
def show_sink_table(spark, table):
    spark.sql(f'select * from {table}').show(truncate=False)


def udf_json_object(col_name, key):
    return F.get_json_object(json.loads(F.col(col_name)), key)

# Auxiliar udf function to deal with escaped characters in value
def value_json(value):
    return json.loads(value)

udf_value_json = F.udf(value_json, T.StringType())

# get DataFrame from table originated by Kafka
def get_table_dataframe(spark, table):
    df_kafka = spark.sql(f'select CAST(value AS STRING), topic, timestamp from {table}')
    
    # notice that value contains escaped characters e.g. \" 
    # "{\"ASIN\": \"1250150183\", \"GROUP\": \"book\", \"FORMAT\": \"hardcover\", 
    # \"TITLE\": \"The Swamp: Washington's Murky Pool of Corruption and Cronyism and How Trump Can Drain It\", 
    # \"AUTHOR\": \"Eric Bolling\", \"PUBLISHER\": \"St. Martin's Press\"}" 
     
    df_kafka = (df_kafka
                .withColumn('jsonvalue', udf_value_json(F.col('value')))
                .withColumn('ID', F.get_json_object(F.col('jsonvalue'), '$.ID'))
                .withColumn('Case', F.get_json_object(F.col('jsonvalue'), '$.Case'))
                .withColumn('Number', F.get_json_object(F.col('jsonvalue'), '$.Number'))
                .withColumn('Date', F.get_json_object(F.col('jsonvalue'), '$.Date'))
                .withColumn('Block', F.get_json_object(F.col('jsonvalue'), '$.Block'))
                .withColumn('IUCR', F.get_json_object(F.col('jsonvalue'), '$.IUCR'))
                .withColumn('Primary_Type', F.get_json_object(F.col('jsonvalue'), '$.Primary_Type'))
                .withColumn('Description', F.get_json_object(F.col('jsonvalue'), '$.Description'))
                .withColumn('Location_Description', F.get_json_object(F.col('jsonvalue'), '$.Location_Description'))
                .withColumn('Arrest', F.get_json_object(F.col('jsonvalue'), '$.Arrest'))
                .withColumn('Domestic', F.get_json_object(F.col('jsonvalue'), '$.Domestic'))
                .withColumn('Beat', F.get_json_object(F.col('jsonvalue'), '$.Beat'))
                .withColumn('District', F.get_json_object(F.col('jsonvalue'), '$.District'))
                .withColumn('Ward', F.get_json_object(F.col('jsonvalue'), '$.Ward'))
                .withColumn('Community_Area', F.get_json_object(F.col('jsonvalue'), '$.Community_Area'))
                .withColumn('FBI_Code', F.get_json_object(F.col('jsonvalue'), '$.FBI_Code'))
                .withColumn('X_Coordinate', F.get_json_object(F.col('jsonvalue'), '$.X_Coordinate'))
                .withColumn('Y_Coordinate', F.get_json_object(F.col('jsonvalue'), '$.Y_Coordinate'))
                .withColumn('Year', F.get_json_object(F.col('jsonvalue'), '$.Year'))
                .withColumn('Updated_On', F.get_json_object(F.col('jsonvalue'), '$.Updated_On'))
                .withColumn('Latitude', F.get_json_object(F.col('jsonvalue'), '$.Latitude'))
                .withColumn('Longitude', F.get_json_object(F.col('jsonvalue'), '$.Longitude'))
                .withColumn('Location', F.get_json_object(F.col('jsonvalue'), '$.Location'))
    )

    return df_kafka

