import re, sys
from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import *
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor


class LTVPredictor:
    """
    Abstraction to run preprocessing and prediction pyspark job
    """
    def __init__(self) -> None:
        self.spark = SparkSession.builder.master('yarn').appName('LTV_Predictor').getOrCreate()

    def load_data(self, folder_id: str) -> DataFrame:
        """
        Method to create the model input

        Parameters:

        `folder_id` : `str`, name of the folder in GCS bucket with the parquet files

        Returns:

        `pyspark.DataFrame` : input for the 
        """
        path = f"gs://processed-data-bucket/{folder_id}"
        COHORTS_DIMENSIONS = ['first_touch_date', 'traffic_source', 'os', 'country']
        TARGET_VAR = 'cohort_ltv_avg_lifetime'

        df_ltv = self.spark.read.parquet(f'{path}/ltv.parquet')
        df_session = self.spark.read.parquet(f'{path}/session.parquet')
        df_event = self.spark.read.parquet(f'{path}/event.parquet')

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
        return df_model

    def predict(self, df_model: DataFrame, folder_id: str) -> None:
        """
        Method to fit the model, make a prediction, and write to GCS bucker folder

        Parameters:

        `df_model` : pyspark.DataFrame, the dataframe obtained from the `load_data` method
        
        `folder_id` : `str`, name of the folder in GCS bucket with the parquet files
        """
        rf = RandomForestRegressor.load('gs://processed-data-bucket/rf')
        rf_fit = rf.fit(df_model)

        prediction = rf_fit.transform(df_model).select(avg('prediction').alias('predicted_ltv'))
        prediction.write.format('csv').option('path', f'gs://processed-data-bucket/{folder_id}/prediction').save(header=True)

    def make_prediction(self, folder_id: str = sys.argv[2]):
        """
        Main method to run the pyspark job
        """
        df = self.load_data(folder_id)
        self.predict(df, folder_id)

if __name__ == '__main__':
    ltvp = LTVPredictor()
    ltvp.make_prediction()