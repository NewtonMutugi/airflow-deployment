from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from includes.load_prep_adverse_events_task import build_load_prep_adverse_events
from includes.load_prep_behaviour_risk_task import build_load_prep_behaviour_risk
from includes.load_prep_care_termination_task import build_load_prep_care_termination
from includes.load_prep_lab_task import build_load_prep_lab
from includes.load_prep_patient_task import build_load_prep_patient
from includes.load_prep_pharmacy_task import build_load_prep_pharmacy
from includes.load_prep_visits_task import build_load_prep_visits

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

spark_ods_url = Variable.get("SPARK_ODS_URL")
spark_ods_driver = Variable.get("SPARK_ODS_DRIVER")
spark_ods_user = Variable.get("SPARK_ODS_USER")
spark_ods_password = Variable.get("SPARK_ODS_PASSWORD")
spark_ods_numpartitions = Variable.get("SPARK_ODS_NUMPARTITIONS")

spark_prepcentral_url = Variable.get("SPARK_PREPCENTRAL_URL")
spark_prepcentral_driver = Variable.get("SPARK_PREPCENTRAL_DRIVER")
spark_prepcentral_user = Variable.get("SPARK_PREPCENTRAL_USER")
spark_prepcentral_password = Variable.get("SPARK_PREPCENTRAL_PASSWORD")
spark_prepcentral_numpartitions = Variable.get("SPARK_PREPCENTRAL_NUMPARTITIONS")


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
    "spark.ods.url": spark_ods_url,
    "spark.ods.driver": spark_ods_driver,
    "spark.ods.user": spark_ods_user,
    "spark.ods.password": spark_ods_password,
    "spark.ods.numpartitions": spark_ods_numpartitions,
    "spark.prepcentral.url": spark_prepcentral_url,
    "spark.prepcentral.driver": spark_prepcentral_driver,
    "spark.prepcentral.user": spark_prepcentral_user,
    "spark.prepcentral.password": spark_prepcentral_password,
    "spark.prepcentral.numpartitions": spark_prepcentral_numpartitions,
    "spark.sql.autoBroadcastJoinThreshold":-1,
    "spark.network.timeout": "1200s",
    "spark.executor.heartbeatInterval": "600s"
}

default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2022, 10, 13, tzinfo=local_tz),
    'email': ['paul.nthusi@thepalladiumgroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='ods_prep_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval=None,
          )

load_prep_adverse_events = build_load_prep_adverse_events(dag=dag, default_conf = default_conf)
load_prep_behaviour_risk = build_load_prep_behaviour_risk(dag=dag, default_conf = default_conf)
load_prep_care_termination = build_load_prep_care_termination(dag=dag, default_conf = default_conf)
load_prep_lab = build_load_prep_lab(dag=dag, default_conf = default_conf)
load_prep_patient = build_load_prep_patient(dag=dag, default_conf = default_conf)
load_prep_pharmacy = build_load_prep_pharmacy(dag=dag, default_conf = default_conf)
load_prep_visits = build_load_prep_visits(dag=dag, default_conf = default_conf)

load_prep_adverse_events >> load_prep_behaviour_risk >> load_prep_care_termination >> load_prep_lab 
load_prep_lab >> load_prep_patient >> load_prep_pharmacy >> load_prep_visits

