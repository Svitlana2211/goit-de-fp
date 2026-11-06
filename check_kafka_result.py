import json

from kafka import KafkaConsumer, TopicPartition

from configs import kafka_config

topic = "svitlana_mishuhli_athlete_enriched_agg"

consumer = KafkaConsumer(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    group_id=None,
    enable_auto_commit=False,
    consumer_timeout_ms=15000,
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

try:
    # Читаємо єдиний розділ створеного нами топіка.
    consumer.assign([TopicPartition(topic, 0)])
    consumer.seek_to_beginning()

    print(f"\nВИХІДНИЙ ТОПІК: {topic}\n")

    count = 0

    for message in consumer:
        count += 1

        print(f"Повідомлення {count}, позиція {message.offset}:")
        print(json.dumps(message.value, ensure_ascii=False))
        print()

        if count == 3:
            break

    if count == 0:
        print("Повідомлень не отримано.")
    else:
        print(f"Перевірено повідомлень: {count}")

finally:
    consumer.close()