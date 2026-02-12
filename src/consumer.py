"""
Module Kafka consumer để lắng nghe và in CDC events từ Debezium.
"""

import json

from kafka import KafkaConsumer


def consume_cdc_events(broker: str, topic: str) -> None:
    """Subscribe vào Kafka topic và in ra mỗi CDC event nhận được.

    Args:
        broker (str): Địa chỉ Kafka broker (host:port).
        topic (str): Tên topic cần subscribe.
    """
    try:
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=broker,
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        )
        print(f"Listening on topic '{topic}'... (Ctrl+C to stop)")

        for message in consumer:
            payload = message.value
            if payload is None:
                continue

            op = payload.get("payload", {}).get("op")
            after = payload.get("payload", {}).get("after")
            op_name = {"c": "INSERT", "u": "UPDATE", "d": "DELETE", "r": "READ"}.get(op, op)

            print(f"[{op_name}] {json.dumps(after, indent=2)}")

    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    except Exception as e:
        raise RuntimeError(f"Failed to consume from topic {topic}: {e}") from e
