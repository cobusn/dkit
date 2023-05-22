"""
Wrapper library around counfluent_kafka
"""

from confluent_kafka import (
    Consumer,
    Producer,
    KafkaError,
    KafkaException,
    TopicPartition,
    Message
)
import time
from typing import Iterator
from logging import getLogger


logger = getLogger("dkit.data.kafka_helper")


class AbstractKafkaClass(object):

    def __init__(self, timeout=1, **config):
        self.timeout = timeout
        self.config = config


class KafkaProducer(AbstractKafkaClass):
    """
    Kafka producer abstraction
    """
    def __init__(self, topic, timeout=1, **config):
        self.topic = topic
        super().__init__(timeout=timeout, **config)
        logger.info(f"producer topic: [{topic}]")
        logger.info(f"producer using config: {self.config}")
        self.producer = Producer(self.config)

    def __del__(self):
        if hasattr(self, "producer") and self.producer:
            logger.info("flushing producer")
            self.producer.flush()

    def send(self, value):
        self.producer.produce(
            self.topic,
            value=value
        )

    def flush(self):
        self.producer.poll()

    def send_and_flush(self, value):
        """send and flush"""
        self.producer.produce(
            self.topic,
            value=value
        )
        self.producer.poll(0)

    def __call__(self, iter_messages: Iterator):
        for msg in iter_messages:
            self.producer.produce(
                self.topic,
                value=msg
            )


class KafkaConsumer(AbstractKafkaClass):
    """
    Consumer abastraction
    """

    def __init__(self, *topics, timeout=1, **config):
        self.topics = list(topics)
        super().__init__(timeout, **config)
        self.consumer: Consumer = None
        self.__init_consumer(self.config)

    def __del__(self):
        if hasattr(self, "consumer") and self.consumer:
            self.consumer.close()

    def __init_consumer(self, config):
        logger.info(f"consumer using topics: [{','.join(self.topics)}]")
        logger.info(f"consumer using config: {config}")
        print(config)
        self.consumer = Consumer(config)
        logger.info(f"subscribing to topics {self.topics}")
        self.consumer.subscribe(self.topics)

    @staticmethod
    def create_config(bootstrap_server, group_id, auto_offset_reset="latest"):
        """helper function to create a valid config dict"""
        return {
            "bootstrap_servers": bootstrap_server,
            "group_id": group_id,
            "auto_offset_reset": auto_offset_reset,
        }

    def commit(self, messages):
        """commit a single message"""
        self.consumer.commit(messages)

    def commit_many(self, messages):
        """commit a list of messges in batch"""
        offsets = [
            TopicPartition(m.topic(), m.partition(), m.offset())
            for m in messages
        ]
        self.consumer.commit(offsets=offsets)

    def fetch(self) -> Message:
        """Return one message from the topic"""
        while True:
            msg = self.consumer.poll(timeout=self.timeout)

            # No message
            if msg is None:
                logger.info("No messages")
                time.sleep(self.timeout)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                return msg

    def iter_fetch(self, n=1) -> Message:
        """yield n messages queue(s)"""
        for i in range(n):
            msg = self.consumer.poll(timeout=self.timeout)

            # No message
            if msg is None:
                logger.info("No messages")
                time.sleep(self.timeout)
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error(
                        '%% %s [%d] reached end at offset %d\n' %
                        (msg.topic(), msg.partition(), msg.offset())
                    )
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                yield msg

    def fetch_many(self, batch_size=100):
        """Retrieve messages in batches"""
        while True:
            try:
                return self.consumer.consume(
                    num_messages=batch_size,
                    timeout=self.timeout
                )
            except Exception as E:
                logger.error(f"iter_fetch/E.__class__.__name__, {E}")
                return []

    def __iter__(self, fetch_chunk=100):
        while True:
            yield self.fetch()


__all__ = [
    KafkaConsumer,
    KafkaProducer,
]
