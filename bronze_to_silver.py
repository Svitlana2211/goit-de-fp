import re
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


project_dir = Path(__file__).resolve().parent
tables = ["athlete_bio", "athlete_event_results"]


def clean_text(text):
    # Зберігаємо відсутні значення як NULL.
    if text is None:
        return None

    # Очищення тексту за правилом із завдання.
    return re.sub(r"""[^a-zA-Z0-9,."'\\]""", "", str(text))


def main():
    spark = (
        SparkSession.builder
        .appName("SvitlanaBronzeToSilver")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")
    clean_text_udf = udf(clean_text, StringType())

    try:
        for table in tables:
            input_path = project_dir / "bronze" / table
            output_path = project_dir / "silver" / table

            # Етап 2.а: читання таблиці з bronze.
            df = spark.read.parquet(str(input_path))
            rows_before = df.count()

            # Етап 2.б: очищення всіх текстових колонок через UDF.
            text_columns = [
                field.name
                for field in df.schema.fields
                if isinstance(field.dataType, StringType)
            ]

            for column_name in text_columns:
                df = df.withColumn(
                    column_name,
                    clean_text_udf(col(column_name)),
                )

            # Етап 2.в: видалення повністю однакових рядків.
            df = df.dropDuplicates()

            # Етап 2.г: запис очищеної таблиці в silver/{table}.
            (
                df.write
                .mode("overwrite")
                .parquet(str(output_path))
            )

            silver_df = spark.read.parquet(str(output_path))
            rows_after = silver_df.count()

            print(f"\nSILVER: {table}", flush=True)
            print(f"Рядків до очищення: {rows_before}", flush=True)
            print(
                f"Рядків після очищення й видалення дублікатів: "
                f"{rows_after}",
                flush=True,
            )
            print(f"Збережено: {output_path}", flush=True)

            silver_df.show(5, truncate=15)

        print("\nЕТАП BRONZE → SILVER ЗАВЕРШЕНО.", flush=True)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()