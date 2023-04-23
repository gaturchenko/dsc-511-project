from pyspark.sql import SparkSession, DataFrame
from py4j.protocol import Py4JJavaError

class bqWrapper:
    """
    Abstraction to create a pyspark session connected to BigQuery
    """
    def __init__(self, master: str = 'yarn', appName: str = 'bigquery-export') -> None:
        self.master = master
        self.appName = appName
        self.connection = self.__create_spark_connection(self.master, self.appName)

    
    @staticmethod
    def __create_spark_connection(master: str, appName: str, project: str = 'snappy-elf-384513', dataset: str = 'dsc_511') -> SparkSession:
        '''
        Initialize connection with spark
        '''
        con = SparkSession.builder.master(master) \
            .appName(appName) \
            .config('spark.jars.packages', 'com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.15.1-beta') \
            .getOrCreate()
        con.conf.set("spark.sql.repl.eagerEval.enabled",True) # reference: https://codelabs.developers.google.com/codelabs/spark-jupyter-dataproc#5
        con.conf.set("viewsEnabled","true")
        con.conf.set("materializationProject",project)
        con.conf.set("materializationDataset",dataset)
        return con
    

    def create_bigquery_connection(self, connection, table: str, project: str = 'snappy-elf-384513', dataset:str = 'dsc_511') -> DataFrame:
        '''
        Read BigQuery table
        Available tables:
            - ltv_data
            - event_data
            - session_data
            - lifetime_data
        '''
        try:
            df = connection.read.format('bigquery') \
                .option('table', f'{project}.{dataset}.{table}') \
                .load()
            return df
        except Py4JJavaError as e:
            print(f'Table {table} does not exist, error:\n{e}')