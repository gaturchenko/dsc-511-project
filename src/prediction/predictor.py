import re, json, sys, os
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *
from google.cloud import storage
from google.oauth2 import service_account
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from loguru import logger
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from kafka_utils import producer


class LTVPredictor:
    def __init__(self) -> None:
        credentials = service_account.Credentials.from_service_account_file(os.path.join(os.path.dirname(__file__), "amazing-source-374915-7739d5755c76.json"))
        self.gcs_client = storage.Client(credentials=credentials)
        self.bucket = self.gcs_client.bucket('processed-data-bucket')

        self.spark = SparkSession.builder.appName('LTV_Predictor').master("spark://spark:7077")\
            .config("spark.jars", os.path.join(os.path.dirname(__file__), "gcs-connector-latest-hadoop2.jar")).getOrCreate()
        self.spark._jsc.hadoopConfiguration().set("google.cloud.auth.service.account.json.keyfile", os.path.join(os.path.dirname(__file__), "application_default_credentials.json"))
        self.spark._jsc.hadoopConfiguration().set('fs.gs.impl', 'com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem')

    def load_data(self, folder_id):
        path = f"gs://processed-data-bucket/{folder_id}"
        COHORTS_DIMENSIONS = ['first_touch_date', 'traffic_source', 'os', 'country']
        TARGET_VAR = 'cohort_ltv_avg_lifetime'

        df_ltv = self.spark.read.parquet(f'{path}/ltv.parquet', header=True)
        df_session = self.spark.read.parquet(f'{path}/session.parquet', header=True)
        df_event = self.spark.read.parquet(f'{path}/event.parquet', header=True)

        df_event = df_event.withColumn('d_cat', when(df_event.d_1 == 1, 'd_1').when(df_event.d_3 == 1, 'd_3').when(df_event.d_5 == 1, 'd_5').\
            when(df_event.d_7 == 1, 'd_7').when((df_event.d_1 != 1)& (df_event.d_3 != 1) & (df_event.d_5 != 1) & (df_event.d_7 != 1), None))

        session_cols = [i for i in df_event.columns if 'session' in i and 'id' not in i]
        agg_dict = dict(zip(session_cols, ['sum' for _ in range(len(session_cols))]))
        df_agg = df_event.groupBy(COHORTS_DIMENSIONS).pivot('d_cat').agg(agg_dict)
        # rename columns which are e.g. d_1_sum(...) to ..._d_1
        df_agg = df_agg.toDF(*[(f"!{i.replace('_sum(', '').replace(')', '')}_"+re.findall(r'd_\d+|null', i)[0]).replace('!'+re.findall(r'd_\d+|null', i)[0], '') if i[i.find('sum'):].replace('sum(', '').replace(')', '') in session_cols else i for i in df_agg.columns])
        # Drop unnecessary columns of type d_<event>_null
        df_agg = df_agg.drop(*[str(col) for col in df_agg.columns if '_null' in col])

        window = Window.partitionBy([col(i) for i in COHORTS_DIMENSIONS]).orderBy(col("cohort_ltv_avg_lifetime").desc())
        df_ltv = df_ltv.withColumn('row', row_number().over(window)) .filter(col('row') == 1).drop('row')

        df = df_ltv.join(df_agg, on=COHORTS_DIMENSIONS, how='left')
        df = df.join(df_session, on=COHORTS_DIMENSIONS, how='left')

        df = df.fillna(0)
        df = df.withColumn('country', when(df.country == '', 'Rest of the World').otherwise(df.country))

        feature_list = [col for col in df.columns if (col not in COHORTS_DIMENSIONS) and (col != TARGET_VAR)]
        assembler = VectorAssembler(inputCols=feature_list, outputCol='features')
        df_model = assembler.transform(df)
        logger.info('Created features')
        return df_model

    def predict(self, df_model, folder_id):
        rf = RandomForestRegressor.load(os.path.join(os.path.dirname(__file__), 'rf'))
        rf_fit = rf.fit(df_model)
        prediction = rf_fit.transform(df_model).select(avg('prediction').alias('predicted_ltv')).toPandas().iloc[0, 0]
        logger.info(f'Predicted LTV: {prediction}')

        message = json.dumps({'data_id': folder_id, 'predicted_ltv': prediction}).encode('utf-8')
        producer.start_producing(message, 'prediction_complete')
        logger.info(f'Sent predicted LTV for the request with ID {folder_id}')

    def make_prediction(self, folder_id):
        df = self.load_data(folder_id)
        self.predict(df, folder_id)

if __name__ == '__main__':
    ltvp = LTVPredictor()
    ltvp.make_prediction('695ebe08-b5e6-4a90-b43d-c8666ba4b49a')