import io, threading, asyncio, concurrent.futures, json, os, sys, uuid, yaml
from google.cloud import bigquery, storage
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from kafka_utils import producer


with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

class BQDataProcessor:
    """
    Abstraction to execute SQL queries in BigQuery in parallel
    """
    config : config
    def __init__(self) -> None:
        self.config = config
        self.bq_client = bigquery.Client(project=config['gcloud']['project'])
        self.gcs_client = storage.Client(project=config['gcloud']['project'])
        self.bucket = self.gcs_client.get_bucket(config['gcloud']['bucket_name'])

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

    def start_job(self, job_name: str, query_params: dict) -> None:
        """
        Method to run the SQL query

        Parameters:

        `job_name` : `str`, one of `["session", "ltv", "event"]`. Maps to the corresponding SQL query

        `query_params` : `dict`, parameters obtained from the application input
        """
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
        """
        Method to parallelize SQL queries execution

        Parameters:

        `params` : `dict`, parameters obtained from the application input
        
        `job_type` : `str`, default "prediction". Used to specify if it is necessary to make a prediction or just \
            query the database
        """
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