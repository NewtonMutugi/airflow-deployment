from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

ldas_spark_sink_dbtable = Variable.get("LDAS_SPARK_SINK_DBTABLE")


def build_load_drug_alcohol_screening_task(dag: DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": ldas_spark_sink_dbtable,
    }
    task_conf.update(default_conf)
    load_drug_alcohol_screening = SparkSubmitOperator(task_id='load_drug_alcohol_screening',
                                                      conn_id=default_conf['connection_id'],
                                                      application=f"{default_conf['spark_app_home']}/load-drug-alcohol-screening-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                                      total_executor_cores=default_conf[
                                                          'default_spark_total_executor_cores'],
                                                      executor_cores=default_conf['default_spark_executor_cores'],
                                                      executor_memory=default_conf['default_spark_executor_memory'],
                                                      driver_memory=default_conf['default_spark_driver_memory'],
                                                      name='load_drug_alcohol_screening',
                                                      conf=task_conf,
                                                      execution_timeout=timedelta(
                                                          minutes=600),
                                                      dag=dag
                                                      )
    return load_drug_alcohol_screening
