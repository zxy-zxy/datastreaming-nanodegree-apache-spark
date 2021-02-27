# Streaming Dataframes, Views and SparkSQL

* get package org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 
from https://mvnrepository.com/artifact/org.apache.spark/spark-sql-kafka-0-10_2.12/3.0.0

* list all topics
```bash
kafka-topics.sh --list --zookeeper localhost:2181
```

* submit spark job
```bash
docker exec -it nd029-c2-apache-spark-and-spark-streaming-starter_spark_1 /bin/bash
```

```bash
/opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 \
/home/workspace/lesson-1-streaming-dataframes/exercises/starter/kafkaconsole.py
```
* consume messages from kafka topic
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic atm-visit-updated --from-beginning
```

