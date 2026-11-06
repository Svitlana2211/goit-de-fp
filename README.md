# Фінальний проєкт з Data Engineering

Авторка: Svitlana Mishuhli.

Проєкт містить дві частини:
- потокову обробку даних через MySQL, Kafka та Spark Structured Streaming;
- пакетну обробку даних із шарами bronze, silver, gold та запуском через Airflow.

## Використані інструменти

- Python 3.12
- Apache Spark / PySpark 3.5.7
- Apache Kafka
- MySQL та MySQL Connector/J 8.0.32
- Apache Airflow 2.10.5
- Docker Compose
- Ubuntu у WSL

## Підготовка Python-середовища

Команди виконуються з кореневої папки проєкту.
Для локального Spark потрібна Java 11 або 17.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Створіть особистий файл налаштувань:

```bash
cp configs.example.py configs.py
```

У `configs.py` заповніть дані доступу до MySQL і Kafka.
Цей файл виключений із Git через `.gitignore`.

Для читання MySQL покладіть у кореневу папку файл
`mysql-connector-j-8.0.32.jar`.

Завантаження драйвера:
https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar

## Частина 1. Потокова обробка

### Етапи

1. Читання `olympic_dataset.athlete_bio` через Spark JDBC.
2. Перетворення зросту та ваги на числа й відбір коректних додатних скінченних значень.
3. Читання `athlete_event_results` із MySQL і передавання JSON-повідомлень у Kafka.
4. Потокове читання Kafka, розбір JSON і з'єднання з даними атлетів за `athlete_id`.
5. Розрахунок середнього зросту та ваги за `sport`, `medal`, `sex`, `country_noc`.
6. Додавання часової мітки та запис через `foreachBatch` у MySQL і вихідний Kafka-топік.

Значення `country_noc` використовується з біографічних даних атлета.

### Топіки та вихідна таблиця

- Вхідний топік: `svitlana_mishuhli_athlete_event_results`
- Вихідний топік: `svitlana_mishuhli_athlete_enriched_agg`
- Таблиця результатів: `svitlana_mishuhli.athlete_enriched_agg`

### Перший запуск

```bash
python create_topics.py
python prepare_database.py
python mysql_to_kafka.py
python streaming_pipeline.py
```

`mysql_to_kafka.py` завантажує вихідні результати змагань.
Повторний запуск цієї програми повторно додасть повідомлення в Kafka.

Потік використовує `availableNow=True`: обробляє доступні повідомлення
та завершує роботу. Контрольні точки зберігаються в `checkpoints/streaming`.

Середні значення накопичуються за всіма обробленими повідомленнями.
У MySQL записується поточний повний результат із заміною таблиці,
а в Kafka додаються повідомлення поточного результату.

Запис у два приймачі не є спільною транзакцією.
За повторного виконання мікробатчу можливі дублікати у вихідному Kafka-топіку.

### Перевірка результатів

```bash
python check_mysql.py
python check_kafka_result.py
```

Під час перевіреного запуску отримано 10 944 групи.

## Частина 2. Пакетна обробка

### Джерела

- https://ftp.goit.study/neoversity/athlete_bio.csv
- https://ftp.goit.study/neoversity/athlete_event_results.csv

### Програми

| Файл | Призначення |
|---|---|
| `landing_to_bronze.py` | Завантаження оригінальних CSV у landing і перетворення на Parquet у bronze |
| `bronze_to_silver.py` | Очищення всіх текстових колонок через UDF та видалення повних дублікатів |
| `silver_to_gold.py` | З'єднання таблиць, відбір коректних фізичних показників і розрахунок середніх значень |
| `project_solution.py` | DAG із трьома послідовними завданнями SparkSubmitOperator |

Результат зберігається в `gold/avg_stats` у форматі Parquet.

Колонки результату:
`sport`, `medal`, `sex`, `country_noc`, `avg_height`, `avg_weight`, `timestamp`.

### Локальна перевірка

```bash
spark-submit --master "local[2]" landing_to_bronze.py
spark-submit --master "local[2]" bronze_to_silver.py
spark-submit --master "local[2]" silver_to_gold.py
```

### Запуск через Airflow та Spark-кластер

Docker Compose запускає три контейнери:
Airflow, Spark master і Spark worker.

Папка проєкту доступна в усіх контейнерах як `/opt/project`.
Це спільне файлове сховище для завдань навчального кластера.

```bash
docker build -t svitlana-airflow-spark:fp .
mkdir -p airflow_runtime/dags airflow_runtime/logs
export LOCAL_UID=$(id -u)
docker compose up -d
docker compose ps
```

Відкрийте Airflow: http://localhost:8081

Ім'я користувача: `admin`.
Після початкового налаштування пароль можна отримати командою:

```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

У меню Admin → Connections створіть підключення:

| Поле | Значення |
|---|---|
| Connection Id | `spark-default` |
| Connection Type | `Spark` |
| Host | `spark://spark-master` |
| Port | `7077` |
| Deploy mode | `client` |

Увімкніть DAG `project_solution` та виконайте один ручний запуск.

Завдання виконуються послідовно:
`landing_to_bronze`, потім `bronze_to_silver`, потім `silver_to_gold`.

Інтерфейс Spark master: http://localhost:8082

Airflow працює в навчальному режимі standalone з SQLite
та SequentialExecutor.

### Результати перевірки

- Усі три завдання DAG завершилися зі статусом `success`.
- Кожна програма виводить результуючу таблицю через `show()`.
- У gold отримано 10 944 групи.

Для зупинення контейнерів проєкту:

```bash
docker compose stop
```

## Підтвердження виконання

До звіту зі скриншотами входять:

1. Результати, прочитані з вихідної таблиці MySQL.
2. Повідомлення з вихідного Kafka-топіку.
3. Таблиця bronze в журналі Airflow.
4. Таблиця silver в журналі Airflow.
5. Таблиця gold в журналі Airflow.
6. Граф успішного запуску DAG.

Дані landing, bronze, silver, gold, службові файли Airflow,
контрольні точки та особисті налаштування виключені з Git.