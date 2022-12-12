from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

lae_spark_sink_dbtable = Variable.get("LAE_SPARK_SINK_DBTABLE")
lae_spark_lookup_adverse = Variable.get("LAE_SPARK_LOOKUP_ADVERSE")
lae_spark_lookup_regimen = Variable.get("LAE_SPARK_LOOKUP_REGIMEN")

def build_load_adverse_events_task(dag: DAG, default_conf):
    task_conf = {
        "spark.sink.dbtable": lae_spark_sink_dbtable,
        "spark.lookup.regimen": lae_spark_lookup_regimen,
        "spark.lookup.adverse": lae_spark_lookup_adverse,
    }
    task_conf.update(default_conf)
    load_adverse_events = SparkSubmitOperator(task_id='load_adverse_events',
                                              conn_id=default_conf['connection_id'],
                                              application=f"{default_conf['spark_app_home']}/load-adverse-events-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                              total_executor_cores=default_conf['default_spark_total_executor_cores'],
                                              executor_cores=default_conf['default_spark_executor_cores'],
                                              executor_memory=default_conf['default_spark_executor_memory'],
                                              driver_memory=default_conf['default_spark_driver_memory'],
                                              name='load_adverse_events',
                                              conf=task_conf,
                                              execution_timeout=timedelta(minutes=600),
                                              dag=dag
                                              )
    return load_adverse_events
