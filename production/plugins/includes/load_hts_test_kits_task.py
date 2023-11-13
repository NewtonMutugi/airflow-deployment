from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

hts_test_kits_ods_spark_dbtable = Variable.get("HTS_TEST_KITS_ODS_SPARK_DBTABLE")

def build_load_hts_test_kits(dag: DAG, default_conf):
    task_conf = {
        "spark.ods.dbtable": hts_test_kits_ods_spark_dbtable,         
    }
    custom_default_conf = default_conf.copy()
    custom_default_conf.update(task_conf)
    load_hts_test_kits = SparkSubmitOperator(task_id='load_hts_test_kits',
                                           conn_id=default_conf['connection_id'],
                                           application=f"{default_conf['spark_app_home']}/load-hts-test-kits-1.0-SNAPSHOT-jar-with-dependencies.jar",
                                           total_executor_cores=default_conf['default_spark_total_executor_cores'],
                                           executor_cores=default_conf['default_spark_executor_cores'],
                                           executor_memory=default_conf['default_spark_executor_memory'],
                                           driver_memory=default_conf['default_spark_driver_memory'],
                                           name='load_hts_test_kits',
                                           conf=custom_default_conf,
                                           execution_timeout=timedelta(minutes=600),
                                           dag=dag
                                           )
    return load_hts_test_kits
