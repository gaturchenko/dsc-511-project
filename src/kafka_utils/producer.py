from kafka import KafkaProducer


def start_producing(message, topic, kafka_host='localhost:9092'):
    producer = KafkaProducer(bootstrap_servers=kafka_host)
    producer.send(topic, message)
    producer.flush()