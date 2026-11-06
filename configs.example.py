# Приклад налаштувань.
# Для запуску скопіюйте цей файл у configs.py
# та заповніть власні дані доступу.

jdbc_url = "jdbc:mysql://217.61.57.46:3306/olympic_dataset"
jdbc_user = "YOUR_MYSQL_USERNAME"
jdbc_password = "YOUR_MYSQL_PASSWORD"

kafka_config = {
    "bootstrap_servers": ["YOUR_KAFKA_HOST:PORT"],
    "username": "YOUR_KAFKA_USERNAME",
    "password": "YOUR_KAFKA_PASSWORD",
    "security_protocol": "YOUR_SECURITY_PROTOCOL",
    "sasl_mechanism": "YOUR_SASL_MECHANISM",
}