from __future__ import annotations

import typing 
from pyspark.sql import SparkSession 
import pendulum
from app.spark_utils import create_local_cluster

if typing.TYPE_CHECKING:
    import pandas as pd 
    

from airflow.decorators import dag, task 

@dag(
    schedule=None,
    start_date=pendulum.datetime(2024,3,20,tz='UTC'),
    catchup=False,
    tags=["spark-app"],
)

def run_spark_app():


    @task
    def spark_task(spark:SparkSession):
        df = spark.range(100)
        df.write.format("delta").mode("overwrite").save("data/range_100.delta")
        return df.toPandas()

    @task
    def df_print(df:pd.DataFrame):
        print(df)
    
    spark = create_spark_session()

    df = spark_task(spark)
    df_print(df)

dag = run_spark_app()
