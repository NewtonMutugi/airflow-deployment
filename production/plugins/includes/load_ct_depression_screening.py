from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

lds_spark_sink_dbtable = Variable.get("LDS_SPARK_SINK_DBTABLE")


def build_load_depression_screening_task(dag: DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": lds_spark_sink_dbtable
    }
    task_conf.update(default_conf)
    load_depression_screening = SparkSubmitOperator(task_id='load_depression_screening',
                                                conn_id=default_conf['connection_id'],
                                                application=f"{default_conf['spark_app_home']}/load-ct-depression-screening-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                                executor_cores=default_conf['default_spark_executor_cores'],
                                                executor_memory=default_conf['default_spark_executor_memory'],
                                                driver_memory=default_conf['default_spark_driver_memory'],
                                                name='load_depression_screening',
                                                conf=task_conf,
                                                execution_timeout=timedelta(
                                                        minutes=600),
                                                dag=dag
                                                )
    return load_depression_screening
