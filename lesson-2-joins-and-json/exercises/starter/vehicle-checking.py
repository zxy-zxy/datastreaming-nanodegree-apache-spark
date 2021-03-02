from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, IntegerType

# TO-DO: create a Vehicle Status kafka message schema StructType including the following JSON elements:
# {"truckNumber":"5169","destination":"Florida","milesFromShop":505,"odomoterReading":50513}

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"

spark = SparkSession.builder.appName("VehicleChecking").master(SPARK_HOST).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

VehicleStatusSchema = StructType([
    StructField("truckNumber", StringType()),
    StructField("destination", StringType()),
    StructField("milesFromShop", IntegerType()),
    StructField("odometerReading", IntegerType())
])

VehicleCheckinSchema = StructType(
    [
        StructField("reservationId", StringType()),
        StructField("locationName", StringType()),
        StructField("truckNumber", StringType()),
        StructField("status", StringType())
    ]
)

vehicleStatusRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "vehicle-status") \
    .option("startingOffsets", "earliest") \
    .load()

vehicleStatusStreamingDF = vehicleStatusRawStreamingDF \
    .selectExpr("cast(key as string)", "cast(value as string)")

vehicleStatusStreamingDF \
    .withColumn("value", F.from_json("value", VehicleStatusSchema)) \
    .select("value.*") \
    .createOrReplaceTempView("VehicleStatus")

vehicleStatusSelectFromView = spark.sql("""
    select
        truckNumber as statusTruckNumber,
        destination, milesFromShop,
        odometerReading
    from VehicleStatus
""")

vehicleCheckingRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "check-in") \
    .option("startingOffsets", "earliest") \
    .load()

vehicleCheckingStreamingDF = vehicleCheckingRawStreamingDF \
    .selectExpr("cast(key as string)", "cast(value as string)")

vehicleCheckingStreamingDF \
    .withColumn("value", F.from_json("value", VehicleCheckinSchema)) \
    .select("value.*") \
    .createOrReplaceTempView("VehicleCheckin")

vehicleCheckingSelectFromView = spark.sql("""
    select
        reservationId,
        locationName,
        truckNumber as checkinTruckNumber,
        status
     from VehicleCheckin
""")

checkingStatusDF = vehicleStatusSelectFromView \
    .join(vehicleCheckingSelectFromView, F.expr("""
            statusTruckNumber = checkinTruckNumber
            """)
          )

consoleOutputStream = checkingStatusDF.writeStream.outputMode("append").format("console").start()
consoleOutputStream.awaitTermination()
