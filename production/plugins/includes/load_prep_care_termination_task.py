from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

prep_care_termination_ods_spark_dbtable = Variable.get("PREP_CARE_TERMINATION_ODS_SPARK_DBTABLE")

def build_load_prep_care_termination(dag: DAG, default_conf):
    task_conf = {
        "spark.ods.dbtable": prep_care_termination_ods_spark_dbtable,         
    }
    custom_default_conf = default_conf.copy()
    custom_default_conf.update(task_conf)
    load_prep_care_termination = SparkSubmitOperator(task_id='load_prep_care_termination',
                                           conn_id=default_conf['connection_id'],
                                           application=f"{default_conf['spark_app_home']}/load-prep-care-termination-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                           total_executor_cores=default_conf['default_spark_total_executor_cores'],
                                           executor_cores=default_conf['default_spark_executor_cores'],
                                           executor_memory=default_conf['default_spark_executor_memory'],
                                           driver_memory=default_conf['default_spark_driver_memory'],
                                           name='load_prep_care_termination',
                                           conf=custom_default_conf,
                                           execution_timeout=timedelta(minutes=600),
                                           dag=dag
                                           )
    return load_prep_care_termination
