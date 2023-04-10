from airflow import DAG
from airflow.operators import BashOperator
from datetime import timedelta


def build_load_intermediate_encounter_hts_tests_task(dag: DAG, default_conf):
    load_intermediate_encounter_hts_tests = BashOperator(task_id='load_intermediate_encounter_hts_tests',
                          dag=dag,
                          bash_command=f"java -jar {default_conf['spark_app_home']}/load-intermediate-encounter-hts-tests-1.0-SNAPSHOT-jar-with-dependencies.jar",
                          env={
                              "ODS_HOST_URL": default_conf['spark.ods.url'],
                              "ODS_USER": default_conf['spark.ods.user'],
                              "ODS_PASSWORD": default_conf['spark.ods.password'],
                              "ODS_QUERY_TIMEOUT": default_conf['spark.intermediateQuery.timeout']
                          },
                          execution_timeout=timedelta(minutes=600)
                          )
    return load_intermediate_encounter_hts_tests
