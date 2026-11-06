from pathlib import Path

import requests
from pyspark.sql import SparkSession


project_dir = Path(__file__).resolve().parent
tables = ["athlete_bio", "athlete_event_results"]


def download_data(table):
    # Етап 1.а: завантаження оригінального CSV.
    landing_dir = project_dir / "landing"
    landing_dir.mkdir(parents=True, exist_ok=True)

    url = f"https://ftp.goit.study/neoversity/{table}.csv"
    local_path = landing_dir / f"{table}.csv"
    temporary_path = landing_dir / f"{table}.csv.part"

    print(f"\nЗавантажуємо: {url}", flush=True)

    with requests.get(
        url,
        stream=True,
        timeout=(15, 180),
    ) as response:
        response.raise_for_status()

        with temporary_path.open("wb") as file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    file.write(chunk)

    temporary_path.replace(local_path)

    print(f"CSV збережено: {local_path}", flush=True)
    return local_path


def main():
    spark = (
        SparkSession.builder
        .appName("SvitlanaLandingToBronze")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    try:
        for table in tables:
            csv_path = download_data(table)

            # Етап 1.б: читання CSV за допомогою Spark.
            df = (
                spark.read
                .option("header", "true")
                .option("inferSchema", "false")
                .option("multiLine", "true")
                .option("escape", '"')
                .option("mode", "FAILFAST")
                .csv(str(csv_path))
            )

            # Етап 1.в: запис у bronze/{table} у форматі Parquet.
            output_path = project_dir / "bronze" / table

            (
                df.write
                .mode("overwrite")
                .parquet(str(output_path))
            )

            print(f"\nBRONZE: {table}", flush=True)
            print(f"Збережено: {output_path}", flush=True)

            # Виведення збереженої таблиці для перевірки та звіту.
            bronze_df = spark.read.parquet(str(output_path))
            bronze_df.show(5, truncate=15)

        print("\nЕТАП LANDING → BRONZE ЗАВЕРШЕНО.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()