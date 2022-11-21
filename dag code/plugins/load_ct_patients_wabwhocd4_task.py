from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

lpwwcd4_spark_sink_dbtable = Variable.get("LPWWCD4_SPARK_SINK_DBTABLE")

def build_load_patient_wab_who_cd4_task(dag: DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": lpwwcd4_spark_sink_dbtable,
    }
    task_conf.update(default_conf)
    load_patient_wab_who_cd4 = SparkSubmitOperator(task_id='load_patient_wab_who_cd4',
                                                   conn_id=default_conf['connection_id'],
                                                   application=f"{default_conf['spark_app_home']}/patients-wab-who-cd4-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                                   total_executor_cores=default_conf[
                                                       'default_spark_total_executor_cores'],
                                                   executor_cores=default_conf['default_spark_executor_cores'],
                                                   executor_memory=default_conf['default_spark_executor_memory'],
                                                   driver_memory=default_conf['default_spark_driver_memory'],
                                                   name='load_patient_wab_who_cd4',
                                                   conf=task_conf,
                                                   execution_timeout=timedelta(
                                                       minutes=600),
                                                   dag=dag
                                                   )
    return load_patient_wab_who_cd4
