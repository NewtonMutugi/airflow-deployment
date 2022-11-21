from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from includes.load_all_facilities_task import build_load_all_facilities_task
from includes.load_art_patients_task import build_load_art_patients_task
from includes.load_ct_patient_visits_task import build_load_ct_patient_visits_task
from includes.load_adverse_events_task import build_load_adverse_events_task
from includes.load_ct_allergies_task import build_load_ct_allergies_task
from includes.load_ct_contact_listing_task import build_load_ct_contact_listing_task
from includes.load_ct_covid_task import build_load_ct_covid_task
from includes.load_ct_defaulter_tracing_task import build_load_ct_defaulter_tracing_task
from includes.load_ct_gbv_screening_task import build_load_ct_gbv_screening_task
from includes.load_ct_ipt_task import build_load_ct_ipt_task
from includes.load_ct_otz_task import build_load_ct_otz_task
from includes.load_ct_ovc_task import build_load_ct_ovc_task
from includes.load_ct_patient_labs_task import build_load_patient_labs_task
from includes.load_ct_patient_status_task import build_load_ct_patient_status_task
from includes.load_ct_patients_task import build_load_ct_patients_task
from includes.load_patient_pharmacy_task import build_load_patient_pharmacy_task
from includes.load_ct_drug_alcohol_screening_task import build_load_drug_alcohol_screening_task
from includes.load_ct_patients_wabwhocd4_task import build_load_patient_wab_who_cd4_task
from includes.load_enhanced_adherence_counselling_task import build_load_enhanced_adherence_counselling_task
from includes.start_ods_etl_task import build_send_ods_etl_start_email_task
from includes.end_ods_etl_task import build_send_ods_etl_end_email_task

local_tz = pendulum.timezone("Africa/Nairobi")

spark_app_home = Variable.get("SPARK_APP_HOME")
spark_driver_port = Variable.get("SPARK_DRIVER_PORT")
spark_driver_block_manager_port = Variable.get(
    "SPARK_DRIVER_BLOCK_MANAGER_PORT")
spark_driver_host = Variable.get("SPARK_DRIVER_HOST")
spark_driver_bind_address = Variable.get("SPARK_DRIVER_BIND_ADDRESS")

default_spark_total_executor_cores = Variable.get(
    "DEFAULT_SPARK_TOTAL_EXECUTOR_CORES")
default_spark_executor_cores = Variable.get("DEFAULT_SPARK_EXECUTOR_CORES")
default_spark_executor_memory = Variable.get("DEFAULT_SPARK_EXECUTOR_MEMORY")
default_spark_driver_memory = Variable.get("DEFAULT_SPARK_DRIVER_MEMORY")

dc_spark_source_database_name = Variable.get("DC_SPARK_SOURCE_DATABASE_NAME")
dc_spark_source_database_host = Variable.get("DC_SPARK_SOURCE_DATABASE_HOST")
dc_spark_source_url = Variable.get("DC_SPARK_SOURCE_URL")
dc_spark_source_driver = Variable.get("DC_SPARK_SOURCE_DRIVER")
dc_spark_source_user = Variable.get("DC_SPARK_SOURCE_USER")
dc_spark_source_password = Variable.get("DC_SPARK_SOURCE_PASSWORD")
dc_spark_source_numpartitions = Variable.get("DC_SPARK_SOURCE_NUMPARTITIONS")
dc_spark_sink_numpartitions = Variable.get("DC_SPARK_SINK_NUMPARTITIONS")
dc_spark_sink_url = Variable.get("DC_SPARK_SINK_URL")
dc_spark_sink_driver = Variable.get("DC_SPARK_SINK_DRIVER")
dc_spark_sink_user = Variable.get("DC_SPARK_SINK_USER")
dc_spark_sink_password = Variable.get("DC_SPARK_SINK_PASSWORD")

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
    "spark.source.database-name": dc_spark_source_database_name,
    "spark.source.database-host": dc_spark_source_database_host,
    "spark.source.url": dc_spark_source_url,
    "spark.source.driver": dc_spark_source_driver,
    "spark.source.user": dc_spark_source_user,
    "spark.source.password": dc_spark_source_password,
    "spark.source.numpartitions": dc_spark_source_numpartitions,
    "spark.sink.url": dc_spark_sink_url,
    "spark.sink.driver": dc_spark_sink_driver,
    "spark.sink.user": dc_spark_sink_user,
    "spark.sink.password": dc_spark_sink_password,
    "spark.sink.numpartitions": dc_spark_sink_numpartitions,
    "spark.sql.autoBroadcastJoinThreshold":-1
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
dag = DAG(dag_id='dwh_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval="0 12 15 * *")

send_ods_etl_start_email = build_send_ods_etl_start_email_task(dag=dag)
send_ods_etl_end_email = build_send_ods_etl_end_email_task(dag=dag)
load_facilities = build_load_all_facilities_task(dag=dag, default_conf = default_conf)
load_art_patients = build_load_art_patients_task(dag=dag, default_conf = default_conf)
load_ct_allergies = build_load_ct_allergies_task(dag=dag, default_conf = default_conf)
load_ct_patient_visits = build_load_ct_patient_visits_task(dag=dag, default_conf = default_conf)
load_adverse_events = build_load_adverse_events_task(dag=dag, default_conf= default_conf)
load_ct_contact_listing = build_load_ct_contact_listing_task(dag=dag, default_conf = default_conf)
load_ct_covid = build_load_ct_covid_task(dag=dag, default_conf = default_conf)
load_ct_defaulter_tracing = build_load_ct_defaulter_tracing_task(dag=dag, default_conf = default_conf)
load_ct_gbv_screening = build_load_ct_gbv_screening_task(dag=dag, default_conf = default_conf)
load_ct_otz = build_load_ct_otz_task(dag=dag, default_conf = default_conf)
load_ct_ovc = build_load_ct_ovc_task(dag=dag, default_conf = default_conf)
load_patient_labs = build_load_patient_labs_task(dag=dag, default_conf = default_conf)
load_ct_patient_status = build_load_ct_patient_status_task(dag=dag, default_conf = default_conf)
load_ct_patients = build_load_ct_patients_task(dag=dag, default_conf = default_conf)
load_patient_pharmacy = build_load_patient_pharmacy_task(dag=dag, default_conf = default_conf)
load_ct_ipt = build_load_ct_ipt_task(dag=dag, default_conf = default_conf)
load_drug_alcohol_screening = build_load_drug_alcohol_screening_task(dag=dag, default_conf = default_conf)
load_patient_wab_who_cd4 = build_load_patient_wab_who_cd4_task(dag=dag, default_conf = default_conf)
load_enhanced_adherence_counselling = build_load_enhanced_adherence_counselling_task(
    dag=dag, default_conf = default_conf)

send_ods_etl_start_email >> load_facilities >> load_ct_patients >> load_art_patients >> load_ct_patient_visits >> load_patient_labs
load_patient_labs >> load_ct_patient_status >> load_patient_pharmacy >> load_adverse_events
load_adverse_events >> load_drug_alcohol_screening >> load_patient_wab_who_cd4 >> load_enhanced_adherence_counselling
load_enhanced_adherence_counselling >> load_ct_allergies >> load_ct_contact_listing >> load_ct_covid >> load_ct_defaulter_tracing
load_ct_defaulter_tracing >> load_ct_gbv_screening >> load_ct_otz >> load_ct_ovc >> load_ct_ipt >> send_ods_etl_end_email
