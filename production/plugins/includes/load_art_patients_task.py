from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

ap_spark_sink_dbtable = Variable.get("AP_SPARK_SINK_DBTABLE")
ap_spark_lookup_regimen = Variable.get("AP_SPARK_LOOKUP_REGIMEN")
ap_spark_lookup_exit_reason = Variable.get("AP_SPARK_LOOKUP_EXIT_REASON")
ap_spark_lookup_patient_source = Variable.get("AP_SPARK_LOOKUP_PATIENT_SOURCE")


def build_load_art_patients_task(dag: DAG, default_conf ):
    task_conf = {
        "spark.sink.dbtable": ap_spark_sink_dbtable,
        "spark.lookup.regimen": ap_spark_lookup_regimen,
        "spark.lookup.exitReason": ap_spark_lookup_exit_reason,
        "spark.lookup.patientSource": ap_spark_lookup_patient_source
    }
    task_conf.update(default_conf)
    load_art_patients = SparkSubmitOperator(task_id='load_art_patients',
                                            conn_id=default_conf['connection_id'],
                                            application=f"{default_conf['spark_app_home']}/load-art-patients-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                            total_executor_cores=default_conf['default_spark_total_executor_cores'],
                                            executor_cores=default_conf['default_spark_executor_cores'],
                                            executor_memory=default_conf['default_spark_executor_memory'],
                                            driver_memory=default_conf['default_spark_driver_memory'],
                                            name='load_art_patients',
                                            conf=task_conf,
                                            execution_timeout=timedelta(
                                                minutes=600),
                                            dag=dag
                                            )
    return load_art_patients
