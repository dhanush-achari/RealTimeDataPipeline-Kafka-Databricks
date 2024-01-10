from confluent_kafka import Consumer, KafkaError, KafkaException
import os
from dotenv import load_dotenv

load_dotenv()
class KafkaConsumer:
    def __init__(self) -> None:
        self.CLOUDKARAFKA_BROKERS = os.getenv('CLOUDKARAFKA_HOSTNAME')
        self.CLOUDKARAFKA_USERNAME = os.getenv('CLOUDKARAFKA_USERNAME_Producer')
        self.CLOUDKARAFKA_PASSWORD = os.getenv('CLOUDKARAFKA_PASSWORD')
        self.CLOUDKARAFKA_TOPIC = os.getenv('CLOUDKARAFKA_TOPIC_NAME')
    def consumer_conf(self):
        consumer_config =  {
                                    'bootstrap.servers': self.CLOUDKARAFKA_BROKERS,
                                    'session.timeout.ms': 6000,
                                    'group.id': f'{self.CLOUDKARAFKA_USERNAME}-consumer',
                                    'default.topic.config': {'auto.offset.reset': 'smallest'},
                                    'security.protocol': 'SASL_SSL',
                                    'sasl.mechanisms': 'SCRAM-SHA-256',
                                    'sasl.username': self.CLOUDKARAFKA_USERNAME,
                                    'sasl.password': self.CLOUDKARAFKA_PASSWORD
                            }
        self.consumer = Consumer(consumer_config)
    
    def run(self):
        self.consumer_conf()
        consumer = self.consumer
        self.running = True
        try:
            consumer.subscribe([self.CLOUDKARAFKA_TOPIC])
            while self.running:
                msg = consumer.poll(1)
                if msg is None:
                    print("No msg")
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f'{msg.topic()}[{msg.partition()}] reached end at offset {msg.offset()}')
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    print(f"Message: {msg}")
        finally:
            consumer.close()
    def shutdown(self):
        self.running = False
