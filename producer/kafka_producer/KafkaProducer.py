import os
from confluent_kafka import Producer
import requests
import json
from dotenv import load_dotenv

load_dotenv()

class KafkaProducer:
    def __init__(self) -> None:
        self.CLOUDKARAFKA_BROKERS = os.getenv('CLOUDKARAFKA_HOSTNAME')
        self.BUS_API_URL = os.getenv('BUS_API')
        self.CLOUDKARAFKA_USERNAME = os.getenv('CLOUDKARAFKA_USERNAME_Producer')
        self.CLOUDKARAFKA_PASSWORD = os.getenv('CLOUDKARAFKA_PASSWORD')
        self.CLOUDKARAFKA_TOPIC = os.getenv('CLOUDKARAFKA_TOPIC_NAME')
        self.API_COLUMNNAME = os.getenv('API_COLUMN_NAME')
   
    def get_data_from_api(self):
        session = requests.Session()
        url = self.BUS_API_URL
        # res = urlopen(url, context=ssl.create_default_context(cafile=certifi.where()))
        # print(res)
        return session.get(url).text
   
    def delivery_callback(self,err, msg):
        if err:
            print(f'Message failed delivery: {err}')
        else:
            print(f'Message delivered to :',msg.topic(), msg.partition())  
   
    def run(self):
        producer_config =  {
                                    'bootstrap.servers': self.CLOUDKARAFKA_BROKERS,
                                    'session.timeout.ms': 6000,
                                    'default.topic.config': {'auto.offset.reset': 'smallest'},
                                    'security.protocol': 'SASL_SSL',
                                    'sasl.mechanisms': 'SCRAM-SHA-256',
                                    'sasl.username': self.CLOUDKARAFKA_USERNAME,
                                    'sasl.password': self.CLOUDKARAFKA_PASSWORD
                            }
        topic_name = self.CLOUDKARAFKA_TOPIC  
        producer = Producer(**producer_config)
        rows_from_api = json.loads(self.get_data_from_api())[self.API_COLUMNNAME]

        initial = """{"message":"""
        final = """}"""
        message = initial + json.dumps(rows_from_api) + final
        print(message)
        producer.produce(topic_name, message, callback=self.delivery_callback)
        producer.poll(5)
        # for row in rows_from_api:
        #     message = json.dumps(row)
        #     producer.produce(topic_name, message, callback=self.delivery_callback)
        #     producer.poll(1)
        # producer.flush()