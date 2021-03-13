from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, DateType, FloatType

KAFKA_HOST = "kafka:19092"
SPARK_HOST = "spark://spark:7077"

# Be sure to specify the option that reads all the events from the topic
# including those that were published before you started the spark stream

spark = SparkSession \
    .builder \
    .master(SPARK_HOST) \
    .appName("stedi-events-console") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

stediEventsRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# parse the JSON from the single column "value" with a json object in it, like this:

stediEventsDF = stediEventsRawDF.selectExpr("cast(value as string) as value")

stediEventsSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

# storing them in a temporary view called CustomerRisk

stediEventsDF \
    .withColumn("value", F.from_json("value", stediEventsSchema)) \
    .select(F.col("value.customer"), F.col("value.score"), F.col("value.riskDate")) \
    .createOrReplaceTempView("CustomerRisk")

# Execute a sql statement against a temporary view, selecting the customer and the
# score from the temporary view, creating a dataframe called customerRiskStreamingDF

customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# It should output like this:
#
# +--------------------+-----
# |customer           |score|
# +--------------------+-----+
# |Spencer.Davis@tes...| 8.0|
# +--------------------+-----

consoleOutput = customerRiskStreamingDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

consoleOutput.awaitTermination()
