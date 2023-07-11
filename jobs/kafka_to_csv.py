from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

def generate_csv(df, batch_id):
    delimiter = "|"
    df = df.selectExpr("topic", "partition", "offset", "CAST(value AS STRING)")
    df_table_data = df.select(
            "topic", "partition", "offset",
            get_json_object(df.value, '$.message.meta.table_name').alias("table_name")
        )

    batch_file_create_time = int(time.time())

    df_table_data.coalesce(3).write.format("csv") \
                .option("header", "true") \
                .option("delimiter", delimiter) \
                .option("ignoreTrailingWhiteSpace",False)\
                .option("ignoreLeadingWhiteSpace",False)\
                .mode('append') \
                .save("/Users/panksingh/Documents/workspace/spark-demo-project/output/{}".format(batch_file_create_time))


def main():
    application_name = "test-app"
    spark = SparkSession.builder \
        .appName("{}".format(application_name)) \
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1') \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.ui.prometheus.enabled', True) \
        .getOrCreate()

    kafka_sasl_username = "username"
    kafka_bootstrap_servers = "server-list"
    topics = "test-spark-topic"
    kafka_sasl_password = "passwd"

    max_records_per_batch = 2000
    trigger_interval = "1 minutes"

    # Define connection params for Kafka
    options = {**option,
        "kafka.sasl.jaas.config": 'org.apache.kafka.common.security.plain.PlainLoginModule required username="{kafka_username}" password="{kafka_pwd}";'.format(
            kafka_username=kafka_sasl_username, kafka_pwd=kafka_sasl_password),
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.bootstrap.servers": kafka_bootstrap_servers,
        "subscribe": topics
    }

    # Set readstream - Structured Stream
    df = spark.readStream \
        .format("kafka") \
        .options(**options) \
        .option("maxOffsetsPerTrigger", max_records_per_batch) \
        .option('failOnDataLoss', 'false') \
        .load()

    query = df.writeStream \
        .foreachBatch(generate_csv) \
        .trigger(processingTime=trigger_interval) \
        .option("checkpointLocation","/Users/panksingh/Documents/workspace/spark-demo-project/checkpoint_storage") \
        .outputMode("append").start().awaitTermination()

if __name__ == '__main__':
    option  = {}
    env = "local"
    main()