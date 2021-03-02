from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, DateType

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"

spark = SparkSession.builder.appName("vehicle-status").master(SPARK_HOST).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

VehicleStatusSchema = StructType(
    [
        StructField("truckNumber", StringType()),
        StructField("destination", StringType()),
        StructField("milesFromShop", StringType()),
        StructField("odometerReading", StringType())
    ]
)

vehicleStatusRawStreamingDF = spark. \
    readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "vehicle-status") \
    .option("startingOffsets", "earliest") \
    .load()

vehicleStatusStreamingDF = vehicleStatusRawStreamingDF \
    .selectExpr("cast(key as string) as key", "cast(value as string) as value")

vehicleStatusStreamingDF \
    .withColumn("value", F.from_json("value", VehicleStatusSchema)) \
    .select(F.col("value.*")) \
    .createOrReplaceTempView("VehicleStatus")

vehicleStatusSelectFromView = spark.sql("select * from VehicleStatus")

consoleStream = vehicleStatusSelectFromView.writeStream.outputMode("append").format("console").start()
consoleStream.awaitTermination()
