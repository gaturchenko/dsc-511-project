import json, redis
from kafka import KafkaConsumer
from loguru import logger


def start_consuming(topic_name='app_request', kafka_host='localhost:9092'):
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)

    consumer = KafkaConsumer(topic_name, bootstrap_servers=kafka_host)
    for msg in consumer:
        message = json.loads(msg.value)
        logger.info(f'Received message with ID {message["request_id"]} and the following data:\n{message["input_data"]}')
        
        r.hset(message["request_id"], mapping={
            'prediction': 1.64
        })
        logger.info(f'Prediction {message["request_id"]} is written to Redis')
        r.close()



if __name__ == '__main__':
    start_consuming()