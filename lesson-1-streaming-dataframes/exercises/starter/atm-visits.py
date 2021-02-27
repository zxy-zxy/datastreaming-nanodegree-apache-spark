from pyspark.sql import SparkSession

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"

spark = SparkSession.builder.appName("atm-visits").master(SPARK_HOST).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

atm_visits_raw_streaming_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "atm-visits") \
    .option("startingOffsets", "earliest") \
    .load()

atm_visits_streaming_df = atm_visits_raw_streaming_df.selectExpr(
    "cast(key as string) as transaction_id", "cast(value as string) as location"
)

atm_visits_streaming_df.createOrReplaceTempView("ATMVisits")

atm_visits_select_from_temp_view_df = spark.sql("select * from ATMVisits")

stream_console = atm_visits_select_from_temp_view_df \
    .selectExpr("cast(transaction_id as string) as key", "cast(location as string) as value") \
    .writeStream.outputMode("append").format("console").start()

stream_kafka = atm_visits_select_from_temp_view_df \
    .selectExpr("cast(transaction_id as string) as key", "cast(location as string) as value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("topic", "atm-visit-updated") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start()

stream_console.awaitTermination()
stream_kafka.awaitTermination()
