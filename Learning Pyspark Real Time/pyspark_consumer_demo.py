scala_version = '2.12'
spark_version = '3.1.2'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.functions import from_json



packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1'
]

spark = SparkSession \
          .builder \
          .appName("APP") \
          .config("spark.jars.packages", ",".join(packages))\
          .getOrCreate()

kafka_df = spark\
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "streaming-topic") \
      .option("startingOffsets", "earliest") \
      .load()\
  
  

kafka_df.printSchema()
# kafka_df_1 = kafka_df.selectExpr("CAST(value AS STRING)", "CAST(timestamp AS TIMESTAMP)")
# userSchema = StructType().add("name", "string").add("age", "integer")

# csvDF = spark \
#     .readStream \
#     .option("sep", ";") \
#     .schema(userSchema) \
#     .csv("/path/to/directory")
kafka_df.selectExpr("value AS json").writeStream.format("json").option("path", "/Desktop/Twitter/test.json").start()
    

