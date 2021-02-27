import pyspark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("balance-updates") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

kafka_raw_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "balance-updates") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_streaming_df = kafka_raw_streaming_df \
    .withColumn("key_casted", F.col("key").cast("string")) \
    .withColumn("value_casted", F.col("value").cast("string")) \
    .select("*")

kafka_streaming_df.createOrReplaceTempView("BalanceUpdatesView")

# this takes the stream and "sinks" it to the console as it is updated one at a time like this:
# +--------------------+-----+
# |                 Key|Value|
# +--------------------+-----+
# |1593939359          |13...|
# +--------------------+-----+

console_stream = kafka_streaming_df.writeStream.outputMode("append").format("console").start()
print("Reading records from kafka")
console_stream.awaitTermination()
