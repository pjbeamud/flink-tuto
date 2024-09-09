from abc import ABC, abstractmethod
import logging
import time

from kafka import KafkaProducer
from pydantic import BaseModel

from src.cybersecurity.generators.basic import BasicGenerator


logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter('%(asctime)s %(message)s', '%Y-%m-%d %H:%M:%S'))

# Add the console handler to the logger
logger.addHandler(console_handler)



class BasicProducer(ABC):
    def __init__(self, topic: str, freq: int):
        self.topic = topic
        self.freq = freq
        self.producer = KafkaProducer(bootstrap_servers='localhost:19092',
                                      value_serializer=lambda data: data.model_dump_json().encode('utf-8'))
        
    @abstractmethod
    def publish(self, *args, **kwargs):
        raise NotImplementedError
        

class CyberSecProducer(BasicProducer):
    def __init__(self, topic: str, freq: int):
        super().__init__(topic, freq)

    def publish(self, message: BaseModel):
        self.producer.send(self.topic, value=message)
        logger.info(f'Produced message on topic {self.topic} with value of {message.model_dump()}')
        time.sleep(self.freq)

def main():
    producer = CyberSecProducer(topic="cybersecurity", freq=2)
    for i in range(100):
        producer.publish(BasicGenerator())
        


if __name__ == '__main__':
    main()