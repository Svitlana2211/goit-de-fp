from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, current_timestamp, isnan


project_dir = Path(__file__).resolve().parent


def main():
    spark = (
        SparkSession.builder
        .appName("SvitlanaSilverToGold")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Етап 3.а: читання двох таблиць із silver.
        bio = spark.read.parquet(
            str(project_dir / "silver" / "athlete_bio")
        )

        events = spark.read.parquet(
            str(project_dir / "silver" / "athlete_event_results")
        )

        # Перетворення ідентифікатора, зросту й ваги на числа.
        bio = bio.select(
            col("athlete_id").cast("long").alias("athlete_id"),
            "sex",
            "country_noc",
            col("height").cast("double").alias("height"),
            col("weight").cast("double").alias("weight"),
        )

        # Відбір записів із коректним зростом і вагою.
        bio = bio.filter(
            col("athlete_id").isNotNull()
            & col("height").isNotNull()
            & col("weight").isNotNull()
            & ~isnan(col("height"))
            & ~isnan(col("weight"))
            & (col("height") > 0)
            & (col("weight") > 0)
            & (col("height") < float("inf"))
            & (col("weight") < float("inf"))
        )

        events = events.select(
            col("athlete_id").cast("long").alias("athlete_id"),
            "sport",
            "medal",
        )

        # Етап 3.б: об’єднання таблиць за athlete_id.
        enriched = events.join(
            bio,
            on="athlete_id",
            how="inner",
        )

        # Етап 3.в: середні показники для кожної комбінації.
        gold_df = (
            enriched
            .groupBy("sport", "medal", "sex", "country_noc")
            .agg(
                avg("height").alias("avg_height"),
                avg("weight").alias("avg_weight"),
            )
            .withColumn("timestamp", current_timestamp())
        )

        # Етап 3.г: запис фінальної таблиці.
        output_path = project_dir / "gold" / "avg_stats"

        (
            gold_df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        # Перевірка збережених результатів.
        saved_df = spark.read.parquet(str(output_path))

        print("\nGOLD: avg_stats", flush=True)
        print(f"Збережено: {output_path}", flush=True)
        print(f"Кількість груп: {saved_df.count()}", flush=True)

        saved_df.show(10, truncate=False)

        print("\nЕТАП SILVER → GOLD ЗАВЕРШЕНО.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()