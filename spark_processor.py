import os
import sys
import json
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, when

# Windows Deployment Workarounds
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
if not os.environ.get("HADOOP_HOME"):
    os.environ["HADOOP_HOME"] = "C:\\hadoop"


def init_spark_session():
    """
    Creates an optimized, isolated Spark Session for local streaming execution.
    """
    return SparkSession.builder \
        .appName("DNS-Forensics-Analysis") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()


def process_stream(spark):
    """
    Consumes raw events from Kafka via native binding and runs the real-time 
    statistical analysis pipeline to detect anomalies.
    """
    spark.sparkContext.setLogLevel("ERROR")
    print("Spark Session active.")
    
    try:
        consumer = KafkaConsumer(
            'dns-traffic',
            bootstrap_servers=['localhost:9092'],
            auto_offset_reset='latest',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("Real-time ingestion listening on topic 'dns-traffic'...")
    except Exception as e:
        print(f"Ingestion interface failed: {e}")
        return

    data_buffer = []
    batch_id = 0

    print("\nWaiting for data streams from producer to process...")

    try:
        for message in consumer:
            data_buffer.append(message.value)
            
            # Micro-batch Trigger: Process chunks of 20 stream events sequentially
            if len(data_buffer) >= 20:
                print(f"\n-------------------------------------------")
                print(f"PROCESSING MICRO-BATCH: {batch_id}")
                print(f"-------------------------------------------")
                
                df = spark.createDataFrame(data_buffer)
                
                # Statistical Aggregation
                df_analysis = df.groupBy("subdomain").agg(mean("length").alias("avg_length"))
                
                # Forensics Threshold Rule: Flagging anomalies based on payload size behavior
                df_alerts = df_analysis.withColumn(
                    "alert_status",
                    when(col("avg_length") > 35, "⚠️ POTENTIAL TUNNELING DETECTED")
                    .otherwise("🟢 NORMAL")
                )
                
                df_alerts.show(truncate=False)
                
                data_buffer.clear()
                batch_id += 1

    except KeyboardInterrupt:
        print("\nAnalysis engine intercepted by user. Terminating process...")
    finally:
        spark.stop()


if __name__ == "__main__":
    print("Booting Data Analytics Engine...")
    session = init_spark_session()
    process_stream(session)