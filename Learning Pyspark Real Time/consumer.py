scala_version = '2.12'
spark_version = '3.1.2'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "streaming-topic"
# KAFKA_OUTPUT_TOPIC_NAME_CONS = "outputtopic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ...")
    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]


    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka Demo") \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages))\
        .config("spark.executor.extraClassPath", ",".join(packages)) \
        .config("spark.executor.extraLibrary", ",".join(packages)) \
        .config("spark.driver.extraClassPath", ",".join(packages)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from testtopic
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    kafka_df.printSchema()

    kafka_df1 = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # # Define a schema for the transaction_detail data
    kafka_schema = StructType().add("value", StringType()) 
    # print(kafka_schema)
    
    kafka_df2 = kafka_df1\
        .select(from_json(col("value"), kafka_schema).alias("value_detail"), "timestamp")
    # print(kafka_df2) 
        

    kafka_df3 = kafka_df2.select("value_detail.*", "timestamp")
    # print(kafka_df3)
    # kafka_df3.show()
    
    trans_detail_write_stream = kafka_df \
        .writeStream \
        .trigger(processingTime='5 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start().awaitTermination()
        

    # # Simple aggregate - find total_transaction_amount by grouping transaction_card_type
    # transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type")\
    #     .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
    #     col("sum(transaction_amount)").alias("total_transaction_amount"))

    # print("Printing Schema of transaction_detail_df4: ")
    # transaction_detail_df4.printSchema()

    # transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100))\
    #                                                 .withColumn("value", concat(lit("{'transaction_card_type': '"), \
    #                                                 col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
    #                                                 col("total_transaction_amount").cast("string"), lit("'}")))

    # print("Printing Schema of transaction_detail_df5: ")
    # transaction_detail_df5.printSchema()

    # Write final result into console for debugging purpose
    # trans_detail_write_stream = transaction_detail_df5 \
    #     .writeStream \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    # trans_detail_write_stream_1 = transaction_detail_df5 \
    #     .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    #     .writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
    #     .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
    #     .trigger(processingTime='1 seconds') \
    #     .outputMode("update") \
    #     .option("checkpointLocation", "file:///D://work//development//spark_structured_streaming_kafka//py_checkpoint") \
    #     .start()

    # trans_detail_write_stream.awaitTermination()

    # print("PySpark Structured Streaming with Kafka Demo Application Completed.")