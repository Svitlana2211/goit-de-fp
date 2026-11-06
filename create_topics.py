from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import kafka_config

topics = [
    "svitlana_mishuhli_athlete_event_results",
    "svitlana_mishuhli_athlete_enriched_agg",
]

admin = KafkaAdminClient(
    bootstrap_servers=kafka_config["bootstrap_servers"],
    security_protocol=kafka_config["security_protocol"],
    sasl_mechanism=kafka_config["sasl_mechanism"],
    sasl_plain_username=kafka_config["username"],
    sasl_plain_password=kafka_config["password"],
    client_id="svitlana_final_project",
    request_timeout_ms=30000,
)

try:
    for name in topics:
        topic = NewTopic(
            name=name,
            num_partitions=1,
            replication_factor=1,
        )

        try:
            admin.create_topics(
                new_topics=[topic],
                timeout_ms=15000,
            )
            print(f"Створено топік: {name}")

        except TopicAlreadyExistsError:
            print(f"Топік уже існує: {name}")

finally:
    admin.close()