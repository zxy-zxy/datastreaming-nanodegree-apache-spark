#!/bin/bash
docker exec -it nd029-c2-apache-spark-and-spark-streaming-starter_spark_1 /opt/bitnami/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 /home/workspace/lesson-2-joins-and-json/exercises/starter/vehicle-checking.py | tee ../../../spark/logs/vehicle-status.log