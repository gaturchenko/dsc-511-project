import sys, os, io, pandas as pd, json, yaml
from loguru import logger
from google.cloud import storage
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from kafka_utils import producer


with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

class PredictionSaver():
    """
    Abstraction to send the completed prediction message
    """
    config : config
    def __init__(self) -> None:
        self.config = config
        self.gcs_client = storage.Client(project=config['gcloud']['project'])
        self.bucket = self.gcs_client.get_bucket(config['gcloud']['bucket_name'])

    def send_prediction(self, folder_id: str) -> None:
        """
        Method to load the prediction from GCS and send the message

        Parameters:

        `folder_id` : `str`, name of the folder in GCS bucket with the parquet files

        """
        for blob in self.gcs_client.list_blobs(config['gcloud']['bucket_name'], prefix=f'{folder_id}/prediction'):
            if '.csv' in blob.name:
                prediction_csv = blob.download_as_string()
        df = pd.read_csv(io.BytesIO(prediction_csv), encoding='utf-8', sep=',')
        message = json.dumps({'data_id': folder_id, 'predicted_ltv': df.iloc[0, 0]}).encode('utf-8')
        producer.start_producing(message, 'prediction_complete')
        logger.info(f'Sent predicted LTV for the request with ID {folder_id}')
        

if __name__ == '__main__':
    ps = PredictionSaver()
    ps.send_prediction('0bad623f-aa2a-4b48-b372-1efa74a7d45f')