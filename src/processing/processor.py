import io, threading, asyncio, concurrent.futures, json, os, sys, uuid
from google.cloud import bigquery, storage
from google.oauth2 import service_account
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from kafka_utils import producer


class BQDataProcessor:
    def __init__(self) -> None:
        credentials = service_account.Credentials.from_service_account_file(os.path.join(os.path.dirname(__file__), "amazing-source-374915-7739d5755c76.json"))
        self.bq_client = bigquery.Client(credentials=credentials)
        self.gcs_client = storage.Client(credentials=credentials)
        self.bucket = self.gcs_client.get_bucket('processed-data-bucket')

        with open(os.path.join(os.path.dirname(__file__), 'session_query.txt'), 'r') as f:
            self.session_query = f.read()

        with open(os.path.join(os.path.dirname(__file__), 'event_query.txt'), 'r') as f:
            self.event_query = f.read()

        with open(os.path.join(os.path.dirname(__file__), 'ltv_query.txt'), 'r') as f:
            self.ltv_query = f.read()
        
        self.query_mapping = {
            'session': self.session_query,
            'event': self.event_query,
            'ltv': self.ltv_query,
        }

    def start_job(self, job_name: str, query_params: dict) -> str:
        query = self.query_mapping[job_name]
        query = query.format(**query_params['input_data'])

        job = self.bq_client.query(query, job_id=f"{job_name}_{query_params['request_id']}", location='US')
        df = job.to_dataframe()

        parquet_file = io.BytesIO()
        df.to_parquet(parquet_file, engine='pyarrow', index=False)
        self.bucket.blob(f"{query_params['request_id']}/{job_name}.parquet").upload_from_string(parquet_file.getvalue(), 'application/octet-stream')

        logger.info(f"Finished job {job_name}_{query_params['request_id']}")
        self.bucket_id = query_params['request_id']

    async def run_threads(self, params: dict, job_type: str = 'prediction') -> None:
        threads = []
        t1 = threading.Thread(target=self.start_job, args=('session', params))
        t2 = threading.Thread(target=self.start_job, args=('event', params))
        t3 = threading.Thread(target=self.start_job, args=('ltv', params))

        threads.append(t1)
        threads.append(t2)
        threads.append(t3)

        t1.start()
        t2.start()
        t3.start()

        loop = asyncio.get_running_loop()
        tasks = []
        for thread in threads:
            task = loop.run_in_executor(None, thread.join)
            tasks.append(task)
        await asyncio.gather(*tasks)
        logger.info('All jobs completed')

        if job_type == 'prediction':
            message = json.dumps({'bucket_folder_id': self.bucket_id}).encode('utf-8')
            producer.start_producing(message, 'prediction_request')
            logger.info(f'Sent prediction request for the bucket folder with ID {self.bucket_id}')


if __name__ == '__main__':
    params = {'request_id': str(uuid.uuid4()), 'input_data': {'os': 'IOS', 'country': 'Cyprus', 'traffic_source': '(none)', 'month': 'August'}}
    bqdp = BQDataProcessor()
    asyncio.run(bqdp.run_threads(params, 'prediction'))