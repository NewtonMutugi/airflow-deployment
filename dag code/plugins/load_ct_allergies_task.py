from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

laci_spark_sink_dbtable = Variable.get("LACI_SPARK_SINK_DBTABLE")
laci_lookup_chronic_illness = Variable.get("LACI_LOOKUP_CHRONIC_ILLNESS")
laci_lookup_allergy_causative_agent = Variable.get(
    "LACI_LOOKUP_ALLERGY_CAUSATIVE_AGENT")
laci_spark_lookup_allergic_reaction = Variable.get(
    "LACI_SPARK_LOOKUP_ALLERGIC_REACTION")


def build_load_ct_allergies_task(dag: DAG, default_conf) -> SparkSubmitOperator:
    task_conf = {
        "spark.sink.dbtable": laci_spark_sink_dbtable,
        "spark.lookup.chronicIllness": laci_lookup_chronic_illness,
        "spark.lookup.allergyCausativeAgent": laci_lookup_allergy_causative_agent,
        "spark.lookup.allergicReaction": laci_spark_lookup_allergic_reaction
    }
    task_conf.update(default_conf)
    load_ct_allergies = SparkSubmitOperator(task_id='load_ct_allergies',
                                            conn_id=default_conf['connection_id'],
                                            application=f"{default_conf['spark_app_home']}/load-ct-allergies-chronic-illness-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                            total_executor_cores=default_conf['default_spark_total_executor_cores'],
                                            executor_cores=default_conf['default_spark_executor_cores'],
                                            executor_memory=default_conf['default_spark_executor_memory'],
                                            driver_memory=default_conf['default_spark_driver_memory'],
                                            name='load_ct_allergies',
                                            conf = task_conf,
                                            execution_timeout=timedelta(
                                                minutes=600),
                                            dag=dag
                                            )
    return load_ct_allergies
