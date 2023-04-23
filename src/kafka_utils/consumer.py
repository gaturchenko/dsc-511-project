import json, redis, os, sys, asyncio, subprocess
from kafka import KafkaConsumer
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from processing.processor import BQDataProcessor
from prediction.saver import PredictionSaver


def start_consuming(topics=['app_request', 'prediction_request', 'prediction_complete'], kafka_host='localhost:9092') -> None:
    """
    Function to start the consumer listening to Kafka server

    Parameters:

    `topics` : `list`, the topics the consumer is listening to. Includes: \
        - Request for SQL queries from the web app
        - Prediction request for the GCS bucket folder
        - Submission of the prediction to Redis

    `kafka_host` : `str`, the address of the kafka host, defaults to localhost:9092
    """
    os.environ['PROJECT'] = 'snappy-elf-384513'
    os.environ['BUCKET_NAME'] = 'processed-data-bucket'
    os.environ['CLUSTER'] = 'cluster-0ad6'
    os.environ['REGION'] = 'europe-west9'

    r = redis.Redis(host='localhost', port=6379, decode_responses=True)
    bqdp = BQDataProcessor()
    ps = PredictionSaver()
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
            logger.info(f"Bucket folder ID {message['bucket_folder_id']} is written to Redis")
            job = f'gcloud dataproc jobs submit pyspark predictor.py \
            --cluster=$CLUSTER \
            --region=$REGION \
            -- gs://$BUCKET_NAME/{message["bucket_folder_id"]} {message["bucket_folder_id"]}'
            subprocess.run([job], shell=True, stdout=subprocess.PIPE, cwd='prediction/')
            ps.send_prediction(message['bucket_folder_id'])

        elif 'data_id' in message.keys():
            logger.info(f"Received a predicted LTV for the data with ID {message['data_id']}")

            r.hset(message["data_id"], mapping={
                'prediction': message['predicted_ltv']
            })
            logger.info(f"Predicted LTV for the data with ID {message['data_id']} is written to Redis")


if __name__ == '__main__':
    logger.info('Consumer activated')
    start_consuming()