from pathlib import Path

from pyspark.sql import SparkSession
from configs import jdbc_url, jdbc_user, jdbc_password

project_dir = Path(__file__).resolve().parent
driver_path = project_dir / "mysql-connector-j-8.0.32.jar"

spark = (
    SparkSession.builder
    .master("local[2]")
    .appName("FinalProjectMySQLCheck")
    .config("spark.jars", str(driver_path))
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:
    for table in ["svitlana_mishuhli.athlete_enriched_agg"]:
        print(f"\nТАБЛИЦЯ: {table}")

        # Читання таблиці з MySQL за допомогою Spark.
        df = (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("connectTimeout", "15000")
            .option("socketTimeout", "60000")
            .load()
        )

        df.printSchema()
        df.show(5, truncate=False)

finally:
    spark.stop()