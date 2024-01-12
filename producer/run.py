from kafka_producer import producer
import time

while True:
    producer.run()
    time.sleep(10)