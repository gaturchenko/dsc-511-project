import uuid, json
from kafka import KafkaProducer, KafkaConsumer
from loguru import logger


def start_producing(message, kafka_host='localhost:9092'):
    producer = KafkaProducer(bootstrap_servers=kafka_host)
    producer.send('app_request', message)
    producer.flush()