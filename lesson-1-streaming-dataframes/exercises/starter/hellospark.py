from pyspark.sql import SparkSession

SPARK_DATA_PATH = 'file:///home/workspace/data'
test_file = f"{SPARK_DATA_PATH}/Test.txt"

spark = (
    SparkSession.builder.appName("HelloSpark")
        .master("spark://spark:7077")
        .getOrCreate()
)
spark.sparkContext.setLogLevel('WARN')

log_data = spark.read.text(test_file).cache()

numAs = 0
numBs = 0


def count_a(row):
    global numAs
    global numBs
    numAs += row.value.count("a")
    numBs += 1
    print(f"Total a count {numAs}")


log_data.foreach(count_a)

num_ds = log_data.filter(log_data.value.contains("d")).count()
num_ss = log_data.filter(log_data.value.contains("s")).count()


def printing(x):
    print(x)


log_data.foreach(printing)
log_data.collect()

res = f"Lines with d: {num_ds}, lines with s: {num_ss}"
print(res)
print(f"Lines with a: {numAs}, lines with b: {numBs}")
spark.stop()
