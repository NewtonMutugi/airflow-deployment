from datetime import datetime, timedelta
from statistics import variance
import pendulum
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.models import Variable

local_tz = pendulum.timezone("Africa/Nairobi")

default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 13, tzinfo=local_tz),
    'email': ['paul.nthusi@thepalladiumgroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='dwh_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 * * * *")

spark_app_home = Variable.get("SPARK_APP_HOME")
spark_driver_port = Variable.get("SPARK_DRIVER_PORT")
spark_driver_block_manager_port = Variable.get("SPARK_DRIVER_BLOCK_MANAGER_PORT")
spark_driver_host = Variable.get("SPARK_DRIVER_HOST")
spark_driver_bind_address = Variable.get("SPARK_DRIVER_BIND_ADDRESS")

lf_spark_source_databse_name = Variable.get("LF_SPARK_SOURCE_DATABSE_NAME")
lf_spark_source_metadata_table = Variable.get("LF_SPARK_SOURCE_METADATA_TABLE")
lf_spark_source_database_host = Variable.get("LF_SPARK_SOURCE_DATABASE_HOST")
lf_spark_source_url = Variable.get("LF_SPARK_SOURCE_URL")
lf_spark_source_driver = Variable.get("LF_SPARK_SOURCE_DRIVER")
lf_spark_source_user = Variable.get("LF_SPARK_SOURCE_USER")
lf_spark_source_password = Variable.get("LF_SPARK_SOURCE_PASSWORD")
lf_spark_source_numpartitions = Variable.get("LF_SPARK_SOURCE_NUMPARTITIONS")
lf_spark_sink_url = Variable.get("LF_SPARK_SINK_URL")
lf_spark_sink_driver = Variable.get("LF_SPARK_SINK_DRIVER")
lf_spark_sink_user = Variable.get("LF_SPARK_SINK_USER")
lf_spark_sink_password = Variable.get("LF_SPARK_SINK_PASSWORD")
lf_spark_sink_dbtable = Variable.get("LF_SPARK_SINK_DBTABLE")

load_facilities = SparkSubmitOperator(task_id='load_facilities',
                                              conn_id='spark_standalone',
                                              application=f'{spark_app_home}/load-all-facilities-1.0-SNAPSHOT-jar-with-dependencies.jar',
                                              total_executor_cores=1,
                                              executor_cores=1,
                                              executor_memory='1g',
                                              driver_memory='1g',
                                              name='load_facilities',
                                              conf={
                                                "spark.driver.port":spark_driver_port,
                                                "spark.driver.blockManager.port":spark_driver_block_manager_port,
                                                "spark.driver.host": spark_driver_host,
                                                "spark.driver.bindAddress": spark_driver_bind_address,
                                                "spark.source.database-name": lf_spark_source_databse_name,
                                                "spark.source.metadata-table": lf_spark_source_metadata_table,
                                                "spark.source.database-host": lf_spark_source_database_host,
                                                "spark.source.url": lf_spark_source_url,
                                                "spark.source.driver": lf_spark_source_driver,
                                                "spark.source.user": lf_spark_source_user,
                                                "spark.source.password": lf_spark_source_password,
                                                "spark.source.numpartitions": lf_spark_source_numpartitions,
                                                "spark.sink.url": lf_spark_sink_url,
                                                "spark.sink.driver": lf_spark_sink_driver,
                                                "spark.sink.user":lf_spark_sink_user ,
                                                "spark.sink.password":lf_spark_sink_password,
                                                "spark.sink.dbtable":lf_spark_sink_dbtable
                                              },
                                              execution_timeout=timedelta(minutes=10),
                                              dag=dag
                                              )
load_facilities