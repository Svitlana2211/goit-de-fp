import pymysql

from configs import jdbc_user, jdbc_password

connection = pymysql.connect(
    host="217.61.57.46",
    port=3306,
    user=jdbc_user,
    password=jdbc_password,
    charset="utf8mb4",
    connect_timeout=15,
    read_timeout=30,
    write_timeout=30,
    autocommit=True,
)

try:
    with connection.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE IF NOT EXISTS `svitlana_mishuhli` "
            "CHARACTER SET utf8mb4"
        )

    print("База svitlana_mishuhli готова для результатів.")

finally:
    connection.close()