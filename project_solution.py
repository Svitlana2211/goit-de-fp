from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import (
    SparkSubmitOperator,
)


# Однаковий шлях до файлів у всіх контейнерах.
PROJECT_DIR = "/opt/project"

# Спільні параметри запуску Spark.
spark_options = {
    "conn_id": "spark-default",
    "deploy_mode": "client",
    "driver_memory": "1g",
    "executor_memory": "1g",
    "executor_cores": 2,
    "total_executor_cores": 2,
    "verbose": True,
    "conf": {
        "spark.driver.host": "airflow",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.pyspark.python": "/usr/local/bin/python",
        "spark.pyspark.driver.python": "/usr/local/bin/python",
        "spark.sql.shuffle.partitions": "8",
        "spark.sql.session.timeZone": "UTC",
    },
}


with DAG(
    dag_id="project_solution",
    description="Обробка даних атлетів: landing → bronze → silver → gold",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "svitlana_mishuhli",
        "retries": 0,
        "execution_timeout": timedelta(minutes=30),
    },
    tags=["final_project", "spark"],
) as dag:

    # Етап 1. Завантаження CSV та збереження у bronze.
    landing_to_bronze = SparkSubmitOperator(
        task_id="landing_to_bronze",
        application=f"{PROJECT_DIR}/landing_to_bronze.py",
        name="landing_to_bronze",
        **spark_options,
    )

    # Етап 2. Очищення тексту та видалення дублікатів.
    bronze_to_silver = SparkSubmitOperator(
        task_id="bronze_to_silver",
        application=f"{PROJECT_DIR}/bronze_to_silver.py",
        name="bronze_to_silver",
        **spark_options,
    )

    # Етап 3. Об'єднання таблиць і розрахунок середніх значень.
    silver_to_gold = SparkSubmitOperator(
        task_id="silver_to_gold",
        application=f"{PROJECT_DIR}/silver_to_gold.py",
        name="silver_to_gold",
        **spark_options,
    )

    # Наступне завдання починається після успішного попереднього.
    landing_to_bronze >> bronze_to_silver >> silver_to_gold