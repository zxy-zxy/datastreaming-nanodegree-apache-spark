from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"

redisMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("value", StringType()),
        StructField("expiredType", StringType()),
        StructField("expiredValue", StringType()),
        StructField("existType", StringType()),
        StructField("ch", StringType()),
        StructField("incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("score", StringType())
            ])
        ))
    ]
)

customerLocationSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("customerId", StringType()),
    ]
)

spark = SparkSession.builder.master(SPARK_HOST).appName("customer-location").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

# this creates a temporary streaming view based on the streaming dataframe
# it can later be queried with spark.sql, we will cover that in the next section
redisServerStreamingDF.withColumn("value", F.from_json("value", redisMessageSchema)) \
    .select(F.col('value.*')) \
    .createOrReplaceTempView("RedisData")

selectStarFromRedisData = spark.sql("select * from RedisData")
zSetEntriesEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as customerLocation from RedisData")

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF.withColumn("customerLocation", F.unbase64(
    zSetEntriesEncodedStreamingDF.customerLocation).cast("string"))

zSetDecodedEntriesStreamingDF \
    .withColumn("customerLocation", F.from_json("customerLocation", customerLocationSchema)) \
    .select(F.col('customerLocation.*')) \
    .createOrReplaceTempView("CustomerLocation")

customerLocationStreamingDF = spark.sql("select * from CustomerLocation")

consoleOutput = customerLocationStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()
consoleOutput.awaitTermination()

# the console output will look something like this:
# +-------------+---------+
# |accountNumber| location|
# +-------------+---------+
# |         null|     null|
# |     93618942|  Nigeria|
# |     55324832|   Canada|
# |     81128888|    Ghana|
# |    440861314|  Alabama|
# |    287931751|  Georgia|
# |    413752943|     Togo|
# |     93618942|Argentina|
# +-------------+---------+
