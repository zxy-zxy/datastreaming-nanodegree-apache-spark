from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType

KAFKA_HOST = "kafka:19092"
SPARK_HOST = "spark://spark:7077"
OUTPUT_TOPIC = "stedi-risk-output-topic"

# Create a StructType for the Kafka redis-server topic
# which has all changes made to Redis - before Spark 3.0.0, schema inference is not automatic
redisRawMessageSchema = StructType(
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", StringType())
            ]))
                    )
    ]
)

# Create a StructType for the Customer JSON that comes
# from Redis- before Spark 3.0.0, schema inference is not automatic
redisCustomerSchema = StructType(
    [
        StructField("customerName", StringType()),
        StructField("email", StringType()),
        StructField("phone", StringType()),
        StructField("birthDay", StringType()),
    ]
)

# create a StructType for the Kafka stedi-events
# topic which has the Customer Risk JSON that comes from Redis- before Spark 3.0.0, schema inference is not automatic

stediEventsSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

# create a spark application object
spark = SparkSession \
    .builder \
    .master(SPARK_HOST) \
    .appName("kafkaRedis") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Using the spark application object, read a streaming dataframe
# from the Kafka topic redis-server as the source
# Be sure to specify the option that reads all the events from the topic including those
# that were published before you started the spark stream
kafkaRedisRawStreamingDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "redis-server") \
    .option("startingOffsets", "earliest") \
    .load()

# cast the value column in the streaming dataframe as a STRING
kafkaRedisStreamingDF = kafkaRedisRawStreamingDF.selectExpr("cast(value as string) as value")

# parse the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"key":"Q3..|
# +------------+
#
# with this JSON format: {"key":"Q3VzdG9tZXI=",
# "existType":"NONE",
# "Ch":false,
# "Incr":false,
# "zSetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "Score":0.0
# }],
# "zsetEntries":[{
# "element":"eyJjdXN0b21lck5hbWUiOiJTYW0gVGVzdCIsImVtYWlsIjoic2FtLnRlc3RAdGVzdC5jb20iLCJwaG9uZSI6IjgwMTU1NTEyMTIiLCJiaXJ0aERheSI6IjIwMDEtMDEtMDMifQ==",
# "score":0.0
# }]
# }
#
# (Note: The Redis Source for Kafka has redundant fields zSetEntries and zsetentries, only one should be parsed)
#
# and create separated fields like this:
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |         key|value|expiredType|expiredValue|existType|   ch| incr|      zSetEntries|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
# |U29ydGVkU2V0| null|       null|        null|     NONE|false|false|[[dGVzdDI=, 0.0]]|
# +------------+-----+-----------+------------+---------+-----+-----+-----------------+
#
# storing them in a temporary view called RedisSortedSet
kafkaRedisStreamingDF \
    .withColumn("value", F.from_json("value", redisRawMessageSchema)) \
    .select(F.col("value.existType"), F.col("value.Ch"), F.col("value.Incr"), F.col("value.zSetEntries")) \
    .createOrReplaceTempView("RedisSortedSet")

# execute a sql statement against a temporary view, which statement takes the element field from
# the 0th element in the array of structs and create a column called encodedCustomer
# the reason we do it this way is that the syntax available select against a view is different than a dataframe,
# and it makes it easy to select the nth element of an array in a sql column
customerEncodedEntriesStreamingDF = spark.sql(
    """
    select zSetEntries[0].element as encodedCustomer from RedisSortedSet
    """)

# TO-DO: take the encodedCustomer column which is base64 encoded at first like this:
# +--------------------+
# |            customer|
# +--------------------+
# |[7B 22 73 74 61 7...|
# +--------------------+

# and convert it to clear json like this:
# +--------------------+
# |            customer|
# +--------------------+
# |{"customerName":"...|
# +--------------------+
#
# with this JSON format:
# {"customerName":"Sam Test","email":"sam.test@test.com","phone":"8015551212","birthDay":"2001-01-03"}

customerDecodedEntriesStreamingDF = customerEncodedEntriesStreamingDF \
    .withColumn("customer", F.unbase64(customerEncodedEntriesStreamingDF.encodedCustomer).cast("string"))

# parse the JSON in the Customer record and store in a temporary view called CustomerRecords
customerDecodedEntriesStreamingDF.withColumn("customer", F.from_json("customer", redisCustomerSchema)) \
    .select(F.col("customer.*")) \
    .createOrReplaceTempView("CustomerRecords")

# JSON parsing will set non-existent fields to null, so let"s select just the fields we
# want, where they are not null as a new dataframe called emailAndBirthDayStreamingDF
emailAndBirthDayStreamingDF = spark.sql(
    """
    select * from CustomerRecords where email is not null AND birthDay is not null
    """
)

# from the emailAndBirthDayStreamingDF dataframe select the email and the birth year (using the split function)
# Split the birth year as a separate field from the birthday
emailAndBirthDayStreamingDF = emailAndBirthDayStreamingDF \
    .withColumn("birthYear", F.split(emailAndBirthDayStreamingDF.birthDay, "-").getItem(0))

# Select only the birth year and email fields as a new streaming data frame called emailAndBirthYearStreamingDF
emailAndBirthYearStreamingDF = emailAndBirthDayStreamingDF.select(F.col("email"), F.col("birthYear"))

stediEventsRawDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("subscribe", "stedi-events") \
    .option("startingOffsets", "earliest") \
    .load()

# cast the value column in the streaming dataframe as a STRING
stediEventsDF = stediEventsRawDF.selectExpr("cast(value as string) as value")

# TO-DO: parse the JSON from the single column "value" with a json object in it, like this:
# +------------+
# | value      |
# +------------+
# |{"custom"...|
# +------------+
#
# and create separated fields like this:
# +------------+-----+-----------+
# |    customer|score| riskDate  |
# +------------+-----+-----------+
# |"sam@tes"...| -1.4| 2020-09...|
# +------------+-----+-----------+
#
# storing them in a temporary view called CustomerRisk
stediEventsDF \
    .withColumn("value", F.from_json("value", stediEventsSchema)) \
    .select(F.col('value.customer'), F.col('value.score'), F.col('value.riskDate')) \
    .createOrReplaceTempView("CustomerRisk")

# execute a sql statement against a temporary view, selecting the customer and
# the score from the temporary view, creating a dataframe called customerRiskStreamingDF
customerRiskStreamingDF = spark.sql("select customer, score from CustomerRisk")

# join the streaming dataframes on the email address to get the risk score and the birth year in the same dataframe
joinStreamingDF = emailAndBirthYearStreamingDF.join(customerRiskStreamingDF, F.expr("email = customer"))

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}

kafkaOutput = joinStreamingDF.selectExpr("CAST(customer AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_HOST) \
    .option("topic", OUTPUT_TOPIC) \
    .option("checkpointLocation", "/tmp/kafkacheckpoint") \
    .start()

kafkaOutput.awaitTermination()
