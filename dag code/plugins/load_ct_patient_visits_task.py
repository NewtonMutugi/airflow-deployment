from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

ctpv_spark_sink_dbtable = Variable.get("CTPV_SPARK_SINK_DBTABLE")
ctpv_spark_lookup_family_planning = Variable.get(
    "CTPV_SPARK_LOOKUP_FAMILY_PLANNING")
ctpv_spark_lookup_pwp = Variable.get("CTPV_SPARK_LOOKUP_PWP")

def build_load_ct_patient_visits_task(dag: DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": ctpv_spark_sink_dbtable,
        "spark.lookup.familyPlanning": ctpv_spark_lookup_family_planning,
        "spark.lookup.pwp": ctpv_spark_lookup_pwp,
    }
    task_conf.update(default_conf)
    load_ct_patient_visits = SparkSubmitOperator(task_id='load_ct_patient_visits',
                                                 conn_id=default_conf['connection_id'],
                                                 application=f"{default_conf['spark_app_home']}/load-ct-patient-visits-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                                 total_executor_cores=default_conf[
                                                     'default_spark_total_executor_cores'],
                                                 executor_cores=default_conf['default_spark_executor_cores'],
                                                 executor_memory=default_conf['default_spark_executor_memory'],
                                                 driver_memory=default_conf['default_spark_driver_memory'],
                                                 name='load_ct_patient_visits',
                                                 conf=task_conf,
                                                 execution_timeout=timedelta(
                                                     minutes=600),
                                                 dag=dag
                                                 )
    return load_ct_patient_visits
