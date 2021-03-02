from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, FloatType, StringType, BooleanType, ArrayType, DateType

SPARK_HOST = "spark://spark:7077"
KAFKA_HOST = "kafka:19092"

spark = SparkSession.builder.master(SPARK_HOST).appName("customer-deposits").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

BankDepositsSchema = StructType([
    StructField("accountNumber", StringType()),
    StructField("amount", FloatType()),
    StructField("dateAndTime", StringType())
])

BankCustomersSchema = StructType([
    StructField("customerName", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("birthDay", StringType()),
    StructField("accountNumber", StringType()),
    StructField("location", StringType()),

])

bankDepositsRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "bank-deposits") \
    .option("startingOffsets", "earliest") \
    .load()

bankDepositsStreamingDF = bankDepositsRawStreamingDF \
    .selectExpr("cast(key as string) as key", "cast(value as string) as value")

bankDepositsStreamingDF \
    .withColumn("value", F.from_json("value", BankDepositsSchema)) \
    .select("value.*") \
    .createOrReplaceTempView("BankDeposits")

bankDepositsSelectFromView = spark.sql(
    """
    select
        accountNumber,
        amount as amountDeposit,
        dateAndTime
    from BankDeposits
    """
)

bankCustomersRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "bank-customers") \
    .option("startingOffsets", "earliest") \
    .load()

bankCustomersStreamingDF = bankCustomersRawStreamingDF \
    .selectExpr("cast(key as string) as key", "cast(value as string) as value")

bankCustomersStreamingDF \
    .withColumn("value", F.from_json("value", BankCustomersSchema)) \
    .select("value.*") \
    .createOrReplaceTempView("BankCustomers")

bankCustomersSelectFromView = spark.sql("""
    select 
        customerName,
        email,
        phone,
        birthDay,
        accountNumber as CustomerAccountNumber,
        location
    from BankCustomers
""")

customersDepositsDF = bankDepositsSelectFromView \
    .join(bankCustomersSelectFromView, F.expr("accountNumber = CustomerAccountNumber"))

consoleOutput = customersDepositsDF \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False).start()
consoleOutput.awaitTermination()

# The console output will look something like this:
# . +-------------+------+--------------------+------------+--------------+
# . |accountNumber|amount|         dateAndTime|customerName|customerNumber|
# . +-------------+------+--------------------+------------+--------------+
# . |    335115395|142.17|Oct 6, 2020 1:59:...| Jacob Doshi|     335115395|
# . |    335115395| 41.52|Oct 6, 2020 2:00:...| Jacob Doshi|     335115395|
# . |    335115395| 261.8|Oct 6, 2020 2:01:...| Jacob Doshi|     335115395|
# . +-------------+------+--------------------+------------+--------------+
