from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan

from configs import jdbc_url, jdbc_user, jdbc_password


def read_athlete_bio(spark):
    # Етап 1: читання фізичних показників атлетів із MySQL.
    bio = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("driver", "com.mysql.cj.jdbc.Driver")
        .option("dbtable", "athlete_bio")
        .option("user", jdbc_user)
        .option("password", jdbc_password)
        .option("connectTimeout", "15000")
        .option("socketTimeout", "60000")
        .load()
    )

    # Етап 2: перетворення зросту й ваги на числа.
    bio = bio.select(
        "athlete_id",
        "sex",
        "country_noc",
        col("height").cast("double").alias("height"),
        col("weight").cast("double").alias("weight"),
    )

    # Видаляємо порожні, некоректні та нескінченні значення.
    bio = bio.filter(
        col("height").isNotNull()
        & col("weight").isNotNull()
        & ~isnan(col("height"))
        & ~isnan(col("weight"))
        & (col("height") > 0)
        & (col("weight") > 0)
        & (col("height") < float("inf"))
        & (col("weight") < float("inf"))
    )

    return bio


if __name__ == "__main__":
    project_dir = Path(__file__).resolve().parent

    spark = (
        SparkSession.builder
        .master("local[2]")
        .appName("SvitlanaAthleteBio")
        .config(
            "spark.jars",
            str(project_dir / "mysql-connector-j-8.0.32.jar"),
        )
        .config("spark.sql.ansi.enabled", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        cleaned_bio = read_athlete_bio(spark)

        print("\nОЧИЩЕНІ ДАНІ АТЛЕТІВ:")
        cleaned_bio.printSchema()
        cleaned_bio.show(10, truncate=False)

    finally:
        spark.stop()