import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, struct, to_json

from configs import jdbc_url, jdbc_user, jdbc_password, kafka_config

project_dir = Path(__file__).resolve().parent
topic = "svitlana_mishuhli_athlete_event_results"

servers = kafka_config["bootstrap_servers"]
if isinstance(servers, (list, tuple)):
    servers = ",".join(servers)

mechanism = kafka_config["sasl_mechanism"]
if mechanism == "PLAIN":
    login_module = "org.apache.kafka.common.security.plain.PlainLoginModule"
else:
    login_module = "org.apache.kafka.common.security.scram.ScramLoginModule"

username = json.dumps(kafka_config["username"], ensure_ascii=False)
password = json.dumps(kafka_config["password"], ensure_ascii=False)

kafka_options = {
    "kafka.bootstrap.servers": servers,
    "kafka.security.protocol": kafka_config["security_protocol"],
    "kafka.sasl.mechanism": mechanism,
    "kafka.sasl.jaas.config": (
        f"{login_module} required "
        f"username={username} password={password};"
    ),
}

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("SvitlanaMySQLToKafka")
    .config(
        "spark.jars",
        str(project_dir / "mysql-connector-j-8.0.32.jar"),
    )
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    # Етап 3.а: читання результатів змагань із MySQL.
    events = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_event_results")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("connectTimeout", "15000")
        .option("socketTimeout", "60000")
        .load()
    )

    # Етап 3.б: перетворення кожного запису на JSON.
    messages = events.select(
        col("athlete_id").cast("string").alias("key"),
        to_json(struct(*events.columns)).alias("value"),
    )

    # Етап 3.в: запис усіх результатів у вхідний Kafka-топік.
    print("Починаємо запис результатів змагань у Kafka...")

    (
        messages.write
        .format("kafka")
        .options(**kafka_options)
        .option("topic", topic)
        .save()
    )

    print(f"ЗАПИС ЗАВЕРШЕНО. Топік: {topic}")

finally:
    spark.stop()