from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"
TOPIC_NAME = "bank-deposits"

spark = SparkSession.builder.master(SPARK_HOST).appName("bank-deposits").getOrCreate()
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("WARN")

bankDepositsSchema = StructType([
    StructField("accountNumber", StringType()),
    StructField("amount", FloatType()),
    StructField("dateAndTime", StringType()),
])

bankDepositsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .load()

bankDepositsStreamingDF = bankDepositsRawStreamingDF \
    .selectExpr("cast(key as string)", "cast(value as string)")

dateFormat = "MMM d, yyyy hh:mm:ss a"

bankDepositsStreamingDF \
    .withColumn("value", F.from_json("value", bankDepositsSchema)) \
    .withColumn("dateAndTmeParsed", F.to_timestamp(F.col("value.dateAndTime"), dateFormat)) \
    .select(F.col("value.*"), F.col("dateAndTmeParsed")) \
    .createOrReplaceTempView("bankDepositsView")

bankDepositsSelectFromView = spark.sql("select * from bankDepositsView")

consoleStream = bankDepositsSelectFromView. \
    writeStream. \
    format("console"). \
    outputMode("append"). \
    option("truncate", False). \
    start()
consoleStream.awaitTermination()

# The console output will look something like this:
# +-------------+------+--------------------+
# |accountNumber|amount|         dateAndTime|
# +-------------+------+--------------------+
# |    103397629| 800.8|Oct 6, 2020 1:27:...|
# +-------------+------+--------------------+
