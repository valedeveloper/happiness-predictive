"""Creación y comprobación de la topología requerida del tópico."""

import argparse

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from .config import BOOTSTRAP_SERVERS, PARTITIONS, REPLICATION_FACTOR, TOPIC


def ensure_topic(bootstrap_servers):
    admin = AdminClient({"bootstrap.servers": bootstrap_servers, "socket.timeout.ms": 10000})
    metadata = admin.list_topics(timeout=15)
    if TOPIC not in metadata.topics:
        future = admin.create_topics([NewTopic(TOPIC, num_partitions=PARTITIONS,
                                                replication_factor=REPLICATION_FACTOR)], request_timeout=15)[TOPIC]
        try:
            future.result()
        except KafkaException as exc:
            if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    topic = admin.list_topics(topic=TOPIC, timeout=15).topics[TOPIC]
    if topic.error:
        raise KafkaException(topic.error)
    if len(topic.partitions) != PARTITIONS or any(
        len(partition.replicas) != REPLICATION_FACTOR for partition in topic.partitions.values()
    ):
        raise RuntimeError(f"{TOPIC} debe tener exactamente {PARTITIONS} particiones y factor de replicación {REPLICATION_FACTOR}.")
    return topic


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-servers", default=BOOTSTRAP_SERVERS)
    args = parser.parse_args()
    topic = ensure_topic(args.bootstrap_servers)
    print(f"{TOPIC}: {len(topic.partitions)} particiones, replicación {REPLICATION_FACTOR}")


if __name__ == "__main__":
    main()
