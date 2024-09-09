from abc import ABC, abstractmethod
import json
import logging
from confluent_kafka import Consumer
from kafka import KafkaConsumer, KafkaProducer

from src.cybersecurity.generators.basic import BasicGenerator

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s', '%Y-%m-%d %H:%M:%S'))

# Add the console handler to the logger
logger.addHandler(console_handler)

class BasicConsumer(ABC):
    def __init__(self, topic: str):
        self.topic = topic
        self.consumer = KafkaConsumer( self.topic,
            bootstrap_servers='localhost:9092',
                            value_deserializer=lambda x: BasicGenerator.model_validate_json(x.decode('utf-8')),
                            group_id='cybersec')
        self.data = []
        
    @abstractmethod
    def handle(self, *args, **kwargs):
        raise NotImplementedError
    

class CyberSecConsumer(BasicConsumer):
    def __init__(self, topic: str):
        super().__init__(topic)

    def handle(self):
        message_count = 0
        for message in self.consumer:
            logger.info(f"Message {message_count}: {message}")
            self.data.append(message)
            message_count += 1

def main():
    consumer = CyberSecConsumer(topic='cybersecurity')
    consumer.handle()

if __name__ == '__main__':
    main()