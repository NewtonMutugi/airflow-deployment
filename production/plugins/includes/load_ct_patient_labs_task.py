from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

lpl_spark_sink_dbtable = Variable.get("LPL_SPARK_SINK_DBTABLE")
lpl_spark_lookup_test_names = Variable.get("LPL_SPARK_LOOKUP_TEST_NAMES")

def build_load_patient_labs_task(dag:DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": lpl_spark_sink_dbtable,
        "spark.lookup.testNames": lpl_spark_lookup_test_names,
    }
    task_conf.update(default_conf)
    load_patient_labs = SparkSubmitOperator(task_id='load_patient_labs',
                                              conn_id= default_conf['connection_id'],
                                              application=f"{default_conf['spark_app_home']}/load-ct-patient-labs-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                              total_executor_cores = default_conf['default_spark_total_executor_cores'],
                                              executor_cores = default_conf['default_spark_executor_cores'],
                                              executor_memory = default_conf['default_spark_executor_memory'],
                                              driver_memory = default_conf['default_spark_driver_memory'],
                                              name='load_patient_labs',
                                              conf = task_conf,
                                              execution_timeout=timedelta(minutes=600),
                                              dag=dag
                                              )
    return load_patient_labs