scala_version = '2.12'
spark_version = '3.1.2'

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

import time

kafka_topic_name = "test-topic"
kafka_bootstrap_servers = 'localhost:9092'
mongodb_host_name = 'localhost'
mongodb_port_no = '27017'
mongodb_user_name = "mongdbuser"
mongodb_password = "Tabinda"
mongodb_database_name = "admin"
mongodb_collection_name = "streaming_data"
# spark_mongodb_output_uri = "mongodb://mongdbuser:Tabinda@localhost:27017/?authMechanism=DEFAULT&authSource=admin"
spark_mongodb_output_uri = "mongodb://" + mongodb_user_name + ":" + mongodb_password + "@" + mongodb_host_name + ":" + mongodb_port_no + "/" + mongodb_database_name + "." + mongodb_collection_name
if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))
    packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1','org.mongodb.spark:mongo-spark-connector_2.12:3.0.1']

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka and Message Format as JSON") \
        .master("local[*]") \
        .config("spark.jars.packages", ",".join(packages))\
        .config("spark.executor.extraClassPath", ",".join(packages)) \
        .config("spark.executor.extraLibrary", ",".join(packages)) \
        .config("spark.driver.extraClassPath", ",".join(packages)) \
        .config("spark.jars.packages",",".join(packages))\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Construct a streaming DataFrame that reads from test-topic
    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # Define a schema for the orders data
    # order_id,order_product_name,order_card_type,order_amount,order_datetime,order_country_name,order_city_name,order_ecommerce_website_name
    orders_schema_string = "ein STRING, strein STRING, name STRING, sub_name STRING, " \
                           + "city STRING, state STRING, ntee_code STRING, " \
                           + "raw_ntee_code STRING, subseccd STRING, has_subseccd STRING, have_filings STRING, have_extracts STRING," \
                           + "have_pdfs STRING, score STRING"

    # 8,Wrist Band,MasterCard,137.13,2020-10-21 18:37:02,United Kingdom,London,www.datamaking.com
    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()

    
    
    # Write final result into console for debugging purpose
    # orders_agg_write_stream = orders_df3 \
    #     .writeStream \
    #     .trigger(processingTime='5 seconds') \
    #     .outputMode("update") \
    #     .option("truncate", "false")\
    #     .format("console") \
    #     .start()

    # orders_agg_write_stream.awaitTermination()

    print("Stream Data Processing Application Completed.")
    


    
    # def func(batch_df, batch_id):
    #     batch_df_1 = batch_df.withColumn("batch_id", lit(batch_id))
    #     batch_df_1.write \
    #     .format("mongo") \
    #     .mode("append") \
    #     .option("uri",spark_mongodb_output_uri) \
    #     .option("dabatbase",mongodb_database_name) \
    #     .option("collection",mongodb_collection_name)\
    #     .save()
    #     batch_df_1.collect()
    def write_row(batch_df , batch_id):
        print("Writhing batch data to mongodb")
        batch_df.write \
        .format("mongo") \
        .mode("append") \
        .option("uri",spark_mongodb_output_uri) \
        .option("dabatbase",mongodb_database_name) \
        .option("collection",mongodb_collection_name) \
        .save()
        pass
        
    orders_df3.writeStream.foreachBatch(write_row).start().awaitTermination()
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    # orders_df3.writeStream \
    #     .trigger(processingTime='30 seconds')\
    #     .outputMode("update") \
    #     .foreachBatch(func(batch_df, batch_id)) \
    #     .start()
    

    
    
