from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

lpp_spark_sink_dbtable = Variable.get("LPP_SPARK_SINK_DBTABLE")
lpp_spark_lookup_regimen = Variable.get("LPP_SPARK_LOOKUP_REGIMEN")
lpp_spark_lookup_treatment = Variable.get("LPP_SPARK_LOOKUP_TREATMENT")
lpp_spark_lookup_prophylaxis = Variable.get("LPP_SPARK_LOOKUP_PROPHYLAXIS")


def build_load_patient_pharmacy_task(dag: DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": lpp_spark_sink_dbtable,
        "spark.lookup.regimen": lpp_spark_lookup_regimen,
        "spark.lookup.treatment": lpp_spark_lookup_treatment,
        "spark.lookup.prophylaxis": lpp_spark_lookup_prophylaxis
    }
    task_conf.update(default_conf)
    load_patient_pharmacy = SparkSubmitOperator(task_id='load_patient_pharmacy',
                                                conn_id=default_conf['connection_id'],
                                                application=f"{default_conf['spark_app_home']}/load-patient-pharmacy-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                                executor_cores=default_conf['default_spark_executor_cores'],
                                                executor_memory=default_conf['default_spark_executor_memory'],
                                                driver_memory=default_conf['default_spark_driver_memory'],
                                                name='load_patient_pharmacy',
                                                conf=task_conf,
                                                execution_timeout=timedelta(
                                                        minutes=600),
                                                dag=dag
                                                )
    return load_patient_pharmacy
