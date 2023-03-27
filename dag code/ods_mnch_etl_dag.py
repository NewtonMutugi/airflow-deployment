from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
from includes.load_ct_patient_baselines_task import build_load_patient_baselines_task
from includes.load_enhanced_adherence_counselling_task import build_load_enhanced_adherence_counselling_task
from includes.load_ct_depression_screening import build_load_depression_screening_task
from includes.load_historical_art_outcome_task import build_load_historical_art_outcome_task

from includes.load_hts_client_linkages_task import build_load_hts_client_linkages
from includes.load_hts_client_tests_task import build_load_hts_client_tests
from includes.load_hts_client_tracing_task  import build_load_hts_client_tracing
from includes.load_hts_clients_task import build_load_hts_clients
from includes.load_hts_eligibility_task import build_load_hts_eligibility
from includes.load_hts_partner_notification_services_task import build_load_hts_partner_notification_services
from includes.load_hts_partner_tracing_task import build_load_hts_partner_tracing
from includes.load_hts_test_kits_task import build_load_hts_test_kits

from includes.load_mnch_anc_visits_task import build_load_mnch_anc_visits
from includes.load_mnch_arts_task import build_load_mnch_arts
from includes.load_mnch_cwc_enrolments_task import build_load_mnch_cwc_enrolments
from includes.load_mnch_cwc_visits_task import build_load_mnch_cwc_visits
from includes.load_mnch_enrolments_task import build_load_mnch_enrolments
from includes.load_mnch_heis_task import build_load_mnch_eis
from includes.load_mnch_labs_task import build_load_mnch_labs
from includes.load_mnch_mat_visits_task import build_load_mnch_mat_visits
from includes.load_mnch_mother_baby_pairs_task import build_load_mnch_mother_baby_pairs
from includes.load_mnch_patients_task import build_load_mnch_patients
from includes.load_mnch_pnc_visits_task import build_load_mnch_pnc_visits

from includes.load_prep_adverse_events_task import build_load_prep_adverse_events
from includes.load_prep_behaviour_risk_task import build_load_prep_behaviour_risk
from includes.load_prep_care_termination_task import build_load_prep_care_termination
from includes.load_prep_lab_task import build_load_prep_lab
from includes.load_prep_patient_task import build_load_prep_patient
from includes.load_prep_pharmacy_task import build_load_prep_pharmacy
from includes.load_prep_visits_task import build_load_prep_visits
from includes.start_ods_etl_task import build_send_ods_etl_start_email_task

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

spark_mnchcentral_url = Variable.get("SPARK_MNCHCENTRAL_URL")
spark_mnchcentral_driver = Variable.get("SPARK_MNCHCENTRAL_DRIVER")
spark_mnchcentral_user = Variable.get("SPARK_MNCHCENTRAL_USER")
spark_mnchcentral_password = Variable.get("SPARK_MNCHCENTRAL_PASSWORD")
spark_mnchcentral_numpartitions = Variable.get("SPARK_MNCHCENTRAL_NUMPARTITIONS")


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
    "spark.mnchcentral.url": spark_mnchcentral_url,
    "spark.mnchcentral.driver": spark_mnchcentral_driver,
    "spark.mnchcentral.user": spark_mnchcentral_user,
    "spark.mnchcentral.password": spark_mnchcentral_password,
    "spark.mnchcentral.numpartitions": spark_mnchcentral_numpartitions,
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
dag = DAG(dag_id='ods_mnch_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval=None,
          )

load_mnch_anc_visits = build_load_mnch_anc_visits(dag=dag, default_conf = default_conf)
load_mnch_arts = build_load_mnch_arts(dag=dag, default_conf = default_conf)
load_mnch_cwc_enrolments = build_load_mnch_cwc_enrolments(dag=dag, default_conf = default_conf)
load_mnch_cwc_visits = build_load_mnch_cwc_visits(dag=dag, default_conf = default_conf)
load_mnch_enrolments = build_load_mnch_enrolments(dag=dag, default_conf = default_conf)
load_mnch_eis = build_load_mnch_eis(dag=dag, default_conf = default_conf)
load_mnch_labs = build_load_mnch_labs(dag=dag, default_conf = default_conf)
load_mnch_mat_visits = build_load_mnch_mat_visits(dag=dag, default_conf = default_conf)
load_mnch_mother_baby_pairs = build_load_mnch_mother_baby_pairs(dag=dag, default_conf = default_conf)
load_mnch_patients = build_load_mnch_patients(dag=dag, default_conf = default_conf)
load_mnch_pnc_visits = build_load_mnch_pnc_visits(dag=dag, default_conf = default_conf)
ods_prep_etl_trigger = TriggerDagRunOperator(
    task_id="trigger_ods_prep_etl",
    trigger_dag_id = "ods_prep_etl_dag",
    dag=dag
)

load_mnch_anc_visits >> load_mnch_arts >> load_mnch_cwc_enrolments >> load_mnch_cwc_visits >> load_mnch_enrolments
load_mnch_enrolments >> load_mnch_eis >> load_mnch_labs >> load_mnch_mat_visits >> load_mnch_mother_baby_pairs
load_mnch_mother_baby_pairs >> load_mnch_patients >> load_mnch_pnc_visits >> ods_prep_etl_trigger

