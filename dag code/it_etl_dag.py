from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from includes.load_intermediate_art_outcomes_task import build_load_intermediate_art_outcomes_task
from includes.load_intermediate_baseline_viral_loads_task import build_load_intermediate_baseline_viral_loads_task
from includes.load_intermediate_last_otz_task import build_load_intermediate_last_otz_task
from includes.load_intermediate_last_ovc_task import build_load_intermediate_last_ovc_task
from includes.load_intermediate_last_visit_task import build_load_intermediate_last_visit_task
from includes.load_intermediate_last_patient_encounter_task import build_load_intermediate_last_patient_encounter_task
from includes.load_intermediate_last_patient_encounter_as_at_task import build_load_intermediate_last_patient_encounter_as_at_task
from includes.load_intermediate_last_pharmacy_dispense_date_task import build_load_intermediate_last_pharmacy_dispense_date_task
from includes.load_intermediate_latest_viral_loads_task import build_load_intermediate_latest_viral_loads_task
from includes.load_intermediate_latest_visit_as_at_task import build_load_intermediate_latest_visit_as_at_task
from includes.load_intermediate_ordered_viral_loads_task import build_load_intermediate_ordered_viral_loads_task
from includes.load_intermediate_pregnancy_as_at_task import build_load_pregnancy_as_at_task
from includes.load_intermediate_pregnancy_during_art_task import build_load_pregnancy_during_art_task
from includes.load_intermediate_latest_weight_height_task import build_load_intermediate_latest_weight_height_task
from includes.load_intermediate_pharmacy_dispense_as_at_task import build_load_pharmacy_dispense_as_at_task
from includes.load_intermediate_encounter_hts_tests_task import build_load_intermediate_encounter_hts_tests_task
from includes.load_intermediate_prep_last_visit_task import build_load_intermediate_prep_last_visit_task
from includes.load_intermediate_latest_prep_assessment_task import build_load_intermediate_prep_assessment_task
from includes.load_intermediate_prep_refills_task import build_load_intermediate_prep_refills_task


local_tz = pendulum.timezone("Africa/Nairobi")

spark_app_home = Variable.get("SPARK_APP_HOME")
spark_driver_port = Variable.get("SPARK_DRIVER_PORT")
spark_driver_block_manager_port = Variable.get("SPARK_DRIVER_BLOCK_MANAGER_PORT")
spark_driver_host = Variable.get("SPARK_DRIVER_HOST")
spark_driver_bind_address = Variable.get("SPARK_DRIVER_BIND_ADDRESS")
default_spark_total_executor_cores = Variable.get("DEFAULT_SPARK_TOTAL_EXECUTOR_CORES")
default_spark_executor_cores = Variable.get("DEFAULT_SPARK_EXECUTOR_CORES")
default_spark_executor_memory = Variable.get("DEFAULT_SPARK_EXECUTOR_MEMORY")
default_spark_driver_memory = Variable.get("DEFAULT_SPARK_DRIVER_MEMORY")

spark_ods_numpartitions = Variable.get("SPARK_ODS_NUMPARTITIONS")
spark_ods_url = Variable.get("SPARK_ODS_URL")
spark_ods_driver = Variable.get("SPARK_ODS_DRIVER")
spark_ods_user = Variable.get("SPARK_ODS_USER")
spark_ods_password = Variable.get("SPARK_ODS_PASSWORD")
spark_intermediate_query_timeout = Variable.get("SPARK_INTERMEDIATE_QUERY_TIMEOUT")


default_conf = {
    "connection_id": "spark_standalone",
    "spark_app_home":spark_app_home,
    "default_spark_total_executor_cores": default_spark_total_executor_cores,
    "default_spark_executor_cores": default_spark_executor_cores,
    "default_spark_executor_memory": default_spark_executor_memory,
    "default_spark_driver_memory": default_spark_driver_memory,
    "spark.driver.port": spark_driver_port,
    "spark.driver.blockManager.port": spark_driver_block_manager_port,
    "spark.driver.host": spark_driver_host,
    "spark.driver.bindAddress": spark_driver_bind_address, 
    "spark.ods.numpartitions":spark_ods_numpartitions, 
    "spark.ods.url": spark_ods_url,
    "spark.ods.driver": spark_ods_driver,
    "spark.ods.user": spark_ods_user,
    "spark.ods.password": spark_ods_password,
    "spark.sink.url": spark_ods_url,
    "spark.sink.driver": spark_ods_driver,
    "spark.sink.user": spark_ods_user,
    "spark.sink.password": spark_ods_password,
    "spark.intermediateQuery.timeout": spark_intermediate_query_timeout,
    "spark.sql.autoBroadcastJoinThreshold":-1
}

default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 25, tzinfo=local_tz),
    'email': ['paul.nthusi@thepalladiumgroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='it_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)

intermediate_art_outcomes = build_load_intermediate_art_outcomes_task(dag = dag, default_conf = default_conf)
baseline_viral_loads = build_load_intermediate_baseline_viral_loads_task(dag = dag, default_conf = default_conf)
intermediate_last_otz = build_load_intermediate_last_otz_task(dag = dag, default_conf = default_conf)
intermediate_last_ovc = build_load_intermediate_last_ovc_task(dag = dag, default_conf = default_conf)
intermediate_last_visit = build_load_intermediate_last_visit_task(dag = dag, default_conf = default_conf)
intermediate_last_patient_encounter = build_load_intermediate_last_patient_encounter_task(dag = dag, default_conf = default_conf)
intermediate_last_patient_encounter_as_at = build_load_intermediate_last_patient_encounter_as_at_task(dag = dag, default_conf = default_conf)
intermediate_last_pharmacy_dispense_date = build_load_intermediate_last_pharmacy_dispense_date_task(dag = dag, default_conf = default_conf)
intermediate_latest_viral_loads = build_load_intermediate_latest_viral_loads_task(dag = dag, default_conf = default_conf)
intermediate_latest_visit_as_at = build_load_intermediate_latest_visit_as_at_task(dag = dag, default_conf = default_conf)
intermediate_ordered_viral_loads = build_load_intermediate_ordered_viral_loads_task(dag = dag, default_conf = default_conf)
load_pregnancy_as_at = build_load_pregnancy_as_at_task(dag = dag, default_conf = default_conf)
load_pregnancy_during_art = build_load_pregnancy_during_art_task(dag = dag, default_conf = default_conf)
intermediate_latest_weight_height = build_load_intermediate_latest_weight_height_task(dag = dag, default_conf = default_conf)
load_pharmacy_dispense_as_at = build_load_pharmacy_dispense_as_at_task(dag = dag, default_conf = default_conf)
load_intermediate_encounter_hts_tests = build_load_intermediate_encounter_hts_tests_task(dag = dag, default_conf = default_conf)
load_intermediate_prep_assessment = build_load_intermediate_prep_assessment_task(dag = dag, default_conf = default_conf)
load_intermediate_prep_last_visit = build_load_intermediate_prep_last_visit_task(dag = dag, default_conf = default_conf)
load_intermediate_prep_refills = build_load_intermediate_prep_refills_task(dag = dag, default_conf = default_conf)

edw_etl_trigger = TriggerDagRunOperator(
    task_id="trigger_edw_etl",
    trigger_dag_id = "edw_dims_etl_dag",
    dag=dag
)
intermediate_latest_visit_as_at  >> baseline_viral_loads >> intermediate_last_otz >> intermediate_last_ovc >> intermediate_last_pharmacy_dispense_date
intermediate_last_pharmacy_dispense_date >> load_pharmacy_dispense_as_at >> intermediate_last_visit >> intermediate_last_patient_encounter_as_at >> intermediate_last_patient_encounter
intermediate_last_patient_encounter >> intermediate_art_outcomes >> intermediate_latest_viral_loads 
intermediate_latest_viral_loads >> intermediate_latest_weight_height >>  intermediate_ordered_viral_loads  
intermediate_ordered_viral_loads >> load_pregnancy_as_at >> load_pregnancy_during_art >> load_intermediate_encounter_hts_tests
load_intermediate_encounter_hts_tests >> load_intermediate_prep_assessment
load_intermediate_prep_assessment >> load_intermediate_prep_last_visit >> load_intermediate_prep_refills >> edw_etl_trigger


