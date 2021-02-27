from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("fuel-level").master("spark://spark:7077").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

kafka_raw_streaming_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:19092") \
    .option("subscribe", "fuel-level") \
    .option("startingOffsets", "earliest") \
    .load()

kafka_streaming_df = kafka_raw_streaming_df.selectExpr("cast(key as string) as key", "cast(value as string) as value")

# this takes the stream and "sinks" it to the console as it is updated one at a time like this:
# +--------------------+-----+
# |                 Key|Value|
# +--------------------+-----+
# |1593939359          |13...|
# +--------------------+-----+

kafka_streaming_df.writeStream.outputMode("append").format("console").start().awaitTermination()
