from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
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

customerJSONSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
        StructField("accountNumber", StringType()),
        StructField("location", StringType())
    ]
)

spark = SparkSession \
    .builder \
    .master(SPARK_HOST) \
    .appName("customer-record") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')

redisServerRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

redisServerStreamingDF = redisServerRawStreamingDF.selectExpr("cast(key as string) key", "cast(value as string) value")

redisServerStreamingDF.withColumn("value", from_json("value", redisMessageSchema)) \
    .select(col('value.*')) \
    .createOrReplaceTempView("RedisData")

# Using spark.sql we can select any valid select statement from the spark view
zSetEntriesEncodedStreamingDF = spark.sql("select key, zSetEntries[0].element as customer from RedisData")

zSetDecodedEntriesStreamingDF = zSetEntriesEncodedStreamingDF \
    .withColumn("customer", unbase64(zSetEntriesEncodedStreamingDF.customer).cast("string"))

zSetDecodedEntriesStreamingDF \
    .withColumn("customer", from_json("customer", customerJSONSchema)) \
    .select(col('customer.*')) \
    .createOrReplaceTempView("Customer")

customerStreamingDF = spark.sql("select accountNumber, location, birthDay from Customer where birthDay is not null")

relevantCustomerFieldsStreamingDF = customerStreamingDF \
    .select(
    'accountNumber',
    'location',
    split(customerStreamingDF.birthDay, "-").getItem(0).alias("birthYear")
)

relevanCustomerFieldsSelect = relevantCustomerFieldsStreamingDF \
    .selectExpr("CAST(accountNumber AS STRING) AS key", "to_json(struct(*)) AS value")

relevantCustomerFieldsStreamingDF.selectExpr("CAST(accountNumber AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("topic", "customer-attributes") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start() \
    .awaitTermination()
