"""Producer base-class providing common utilites and functionality"""
import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    BROKER_URL = "PLAINTEXT://localhost:9092,PLAINTEXT://localhost:9093,PLAINTEXT://localhost:9094"
    SCHEMA_REGISTRY_URL = "http://127.0.0.1:8081"

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas
        self.kafka_admin_client = AdminClient({
            "bootstrap.servers": Producer.BROKER_URL
        })


        self.broker_properties = {
            "bootstrap.servers": Producer.BROKER_URL,
            "schema.registry.url": Producer.SCHEMA_REGISTRY_URL
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        self.producer = AvroProducer(
            self.broker_properties
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        if self.topic_exists() is True:
            logger.info(f"Topic {self.topic_name} already exists.")
            return

        futures = self.kafka_admin_client.create_topics(
            [
                NewTopic(
                    topic=self.topic_name,
                    num_partitions=self.num_partitions,
                    replication_factor=self.num_replicas,
                    config={
                        "cleanup.policy": "delete",
                        "compression.type": "lz4"
                    }
                )
            ]
        )

        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"Topic {self.topic_name} Created!")
            except Exception as e:
                logger.info(f"Failed to Create Topic {self.topic_name}: {e}")
                raise

    def topic_exists(self):
        """Checks if the given topic exists"""
        cluster_metadata = self.kafka_admin_client.list_topics(timeout=5)
        return cluster_metadata.topics.get(self.topic_name) is not None

    def time_millis(self):
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        if self.producer is None:
            return

        logger.debug("flushing producer...")
        self.producer.flush()
