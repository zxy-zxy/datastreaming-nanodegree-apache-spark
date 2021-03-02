from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"

spark = SparkSession.builder.appName("bank-withdrawals").master(SPARK_HOST).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bankWithdrawalsSchema = StructType(
    [
        StructField("accountNumber", StringType()),
        StructField("amount", FloatType()),
        StructField("dateAndTime", StringType()),
        StructField("transactionId", StringType()),
    ]
)

atmWithdrawalsSchema = StructType(
    [
        StructField("transactionDate", StringType()),
        StructField("transactionId", StringType()),
        StructField("atmLocation", StringType()),
    ]
)

bankWithdrawalsRawStreamingDF = spark \
    .readStream \
    .format("kafka").option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "bank-withdrawals") \
    .option("startingOffsets", "earliest") \
    .load()

bankWithdrawalsStreamingDF = bankWithdrawalsRawStreamingDF.selectExpr("cast(key as string)", "cast(value as string)")
bankWithdrawalsStreamingDF \
    .withColumn("value", F.from_json("value", bankWithdrawalsSchema)) \
    .select("value.*") \
    .createOrReplaceTempView("BankWithdrawals")

bankWithdrawalsSelectFromView = spark.sql(
    """
    select * from BankWithdrawals
    """
)

atmWithdrawalsRawStreamingDF = spark \
    .readStream \
    .format("kafka").option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "atm-withdrawals") \
    .option("startingOffsets", "earliest") \
    .load()

atmWithdrawalsStreamingDF = atmWithdrawalsRawStreamingDF.selectExpr("cast(key as string)", "cast(value as string)")
atmWithdrawalsStreamingDF \
    .withColumn("value", F.from_json("value", atmWithdrawalsSchema)) \
    .select("value.*") \
    .createOrReplaceTempView("atmWithdrawals")

atmWithdrawalsSelectFromView = spark.sql(
    """
    select 
        transactionDate,
        transactionId as atmTransactionID,
        atmLocation
    from atmWithdrawals
    """
)

atmAndBankwithdrawals = bankWithdrawalsSelectFromView \
    .join(atmWithdrawalsSelectFromView, F.expr("transactionId = atmTransactionID"))

consoleStream = atmAndBankwithdrawals \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

kafkaStream = atmAndBankwithdrawals \
    .selectExpr("cast(transactionId as string) as key", "to_json(struct(*)) as value") \
    .writeStream.format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("topic", "withdrawals-location") \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start()

consoleStream.awaitTermination()
kafkaStream.awaitTermination()
