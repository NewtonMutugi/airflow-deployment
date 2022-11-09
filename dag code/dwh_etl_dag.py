from datetime import datetime, timedelta
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
from includes.start_ods_etl_task import build_send_ods_etl_start_email_task
from includes.end_ods_etl_task import build_send_ods_etl_end_email_task

local_tz = pendulum.timezone("Africa/Nairobi")

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

send_ods_etl_start_email = build_send_ods_etl_start_email_task(dag = dag)
send_ods_etl_end_email = build_send_ods_etl_end_email_task(dag = dag)
# load_facilities = build_load_all_facilities_task(dag = dag)
# load_art_patients = build_load_art_patients_task(dag = dag)
# load_ct_allergies = build_load_ct_allergies_task(dag = dag)
# load_ct_patient_visits = build_load_ct_patient_visits_task(dag = dag)
# load_adverse_events = build_load_adverse_events_task(dag = dag)
# load_ct_contact_listing = build_load_ct_contact_listing_task(dag = dag)
# load_ct_covid = build_load_ct_covid_task(dag = dag)
# load_ct_defaulter_tracing = build_load_ct_defaulter_tracing_task(dag = dag)
# load_ct_gbv_screening = build_load_ct_gbv_screening_task(dag = dag)
# load_ct_otz = build_load_ct_otz_task(dag = dag)
# load_ct_ovc = build_load_ct_ovc_task(dag = dag)
# load_patient_labs = build_load_patient_labs_task(dag = dag)
# load_ct_patient_status = build_load_ct_patient_status_task(dag = dag)
# load_ct_patients = build_load_ct_patients_task(dag = dag)
# load_patient_pharmacy = build_load_patient_pharmacy_task(dag = dag)
# load_ct_ipt = build_load_ct_ipt_task(dag = dag)

send_ods_etl_start_email >> send_ods_etl_end_email
 
# send_ods_etl_start_email >> load_facilities >>  load_ct_patients >> load_art_patients >> load_ct_patient_visits  >> load_patient_labs
# load_patient_labs >> load_ct_patient_status >> load_patient_pharmacy >> load_adverse_events 
# load_adverse_events >> load_ct_allergies >> load_ct_contact_listing >> load_ct_covid >> load_ct_defaulter_tracing
# load_ct_defaulter_tracing >> load_ct_gbv_screening >> load_ct_otz >> load_ct_ovc >> load_ct_ipt >> send_ods_etl_end_email