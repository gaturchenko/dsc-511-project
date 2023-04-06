import json, redis, os, sys, asyncio
from kafka import KafkaConsumer
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processing.processor import BQDataProcessor


def start_consuming(topics=['app_request', 'prediction_request'], kafka_host='localhost:9092'):
    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    bqdp = BQDataProcessor()
    
    consumer = KafkaConsumer(bootstrap_servers=kafka_host)
    consumer.subscribe(topics)
    for msg in consumer:
        message = json.loads(msg.value)
        if 'request_id' in message.keys():
            logger.info(f'Received message with ID {message["request_id"]} and the following data:\n{message["input_data"]}')
            
            asyncio.run(bqdp.run_threads(message, 'prediction'))

        elif 'bucket_folder_id' in message.keys():
            logger.info(f"Received a prediction request with bucket folder ID {message['bucket_folder_id']}")

            r.hset(message["bucket_folder_id"], mapping={
                'bucket_folder_id': message['bucket_folder_id']
            })
            logger.info(f"bucket folder ID {message['bucket_folder_id']} is written to Redis")
            r.close()
            

if __name__ == '__main__':
    start_consuming()