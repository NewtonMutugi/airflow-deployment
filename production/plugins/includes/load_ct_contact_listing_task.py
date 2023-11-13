from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

lcl_spark_sink_dbtable = Variable.get("LCL_SPARK_SINK_DBTABLE")

def build_load_ct_contact_listing_task(dag: DAG, default_conf) -> SparkSubmitOperator:
    task_conf = {
        "spark.sink.dbtable": lcl_spark_sink_dbtable,
    }
    task_conf.update(default_conf)
    load_ct_contact_listing = SparkSubmitOperator(task_id='load_ct_contact_listing',
                                                  conn_id='spark_standalone',
                                                  application=f"{default_conf['spark_app_home']}/load-ct-contact-listing-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                                  total_executor_cores=default_conf[
                                                      'default_spark_total_executor_cores'],
                                                  executor_cores=default_conf['default_spark_executor_cores'],
                                                  executor_memory=default_conf['default_spark_executor_memory'],
                                                  driver_memory=default_conf['default_spark_driver_memory'],
                                                  name='load_ct_contact_listing',
                                                  conf=task_conf,
                                                  execution_timeout=timedelta(
                                                      minutes=600),
                                                  dag=dag
                                                  )
    return load_ct_contact_listing
