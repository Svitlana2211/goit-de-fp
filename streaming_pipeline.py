import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    broadcast,
    col,
    current_timestamp,
    from_json,
    struct,
    to_json,
)

from athlete_bio import read_athlete_bio
from configs import jdbc_user, jdbc_password, kafka_config


project_dir = Path(__file__).resolve().parent

input_topic = "svitlana_mishuhli_athlete_event_results"
output_topic = "svitlana_mishuhli_athlete_enriched_agg"

output_url = "jdbc:mysql://217.61.57.46:3306/svitlana_mishuhli"
output_table = "athlete_enriched_agg"

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
    .appName("SvitlanaStreamingPipeline")
    .config(
        "spark.jars",
        str(project_dir / "mysql-connector-j-8.0.32.jar"),
    )
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7",
    )
    .config("spark.sql.ansi.enabled", "false")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


def foreach_batch_function(batch_df, batch_id):
    # Час виконання розрахунків для поточного мікробатчу.
    result = (
        batch_df
        .withColumn("timestamp", current_timestamp())
        .persist()
    )

    try:
        group_count = result.count()

        if group_count == 0:
            print(f"Мікробатч {batch_id}: результат порожній.")
            return

        # Етап 6.б: запис повного актуального результату в MySQL.
        (
            result.coalesce(1).write
            .format("jdbc")
            .option("url", output_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", output_table)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("connectTimeout", "15000")
            .option("socketTimeout", "60000")
            .mode("overwrite")
            .save()
        )

        # Етап 6.а: запис результатів у вихідний Kafka-топік.
        kafka_result = result.select(
            to_json(
                struct("sport", "medal", "sex", "country_noc")
            ).alias("key"),
            to_json(struct(*result.columns)).alias("value"),
        )

        (
            kafka_result.write
            .format("kafka")
            .options(**kafka_options)
            .option("topic", output_topic)
            .save()
        )

        print(
            f"\nМікробатч {batch_id}: "
            f"записано {group_count} груп у MySQL та Kafka."
        )
        result.show(10, truncate=False)

    finally:
        result.unpersist()


bio = None

try:
    # Етапи 1–2: читання й очищення даних атлетів.
    # Реалізація цих етапів міститься у файлі athlete_bio.py.
    bio = read_athlete_bio(spark).cache()
    print(f"Завантажено очищених записів атлетів: {bio.count()}")

    # Етап 3: потокове читання результатів змагань із Kafka.
    raw_stream = (
        spark.readStream
        .format("kafka")
        .options(**kafka_options)
        .option("subscribe", input_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Перетворюємо всі поля JSON на окремі колонки.
    event_schema = """
        edition STRING,
        edition_id INT,
        country_noc STRING,
        sport STRING,
        event STRING,
        result_id BIGINT,
        athlete STRING,
        athlete_id INT,
        pos STRING,
        medal STRING,
        isTeamSport STRING
    """

    events = (
        raw_stream
        .select(
            from_json(
                col("value").cast("string"),
                event_schema,
            ).alias("data")
        )
        .select("data.*")
    )

    # Етап 4: об’єднання потоку з даними атлетів за athlete_id.
    enriched = (
        events.alias("e")
        .join(
            broadcast(bio.alias("b")),
            col("e.athlete_id") == col("b.athlete_id"),
            "inner",
        )
        .select(
            col("e.sport").alias("sport"),
            col("e.medal").alias("medal"),
            col("b.sex").alias("sex"),
            col("b.country_noc").alias("country_noc"),
            col("b.height").alias("height"),
            col("b.weight").alias("weight"),
        )
    )

    # Етап 5: накопичувальні середні за всіма обробленими даними.
    averages = (
        enriched
        .groupBy("sport", "medal", "sex", "country_noc")
        .agg(
            avg("height").alias("avg_height"),
            avg("weight").alias("avg_weight"),
        )
    )

    # Етап 6: оброблення мікробатчів із записом у два місця.
    query = (
        averages.writeStream
        .outputMode("complete")
        .foreachBatch(foreach_batch_function)
        .option(
            "checkpointLocation",
            str(project_dir / "checkpoints" / "streaming"),
        )
        .trigger(availableNow=True)
        .start()
    )

    query.awaitTermination()
    print("\nПОТІК ЗАВЕРШИВ РОБОТУ.")

finally:
    if bio is not None:
        bio.unpersist()

    spark.stop()