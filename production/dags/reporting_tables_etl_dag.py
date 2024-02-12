from airflow import DAG
import pendulum
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from includes.reporting.load_all_emr_sites_task import build_load_all_emr_sites_task
from includes.reporting.load_linelist_adverse_events_task import build_load_linelist_adverse_events_task
from includes.reporting.load_linelist_appointments_task import build_load_linelist_appointments_task
from includes.reporting.load_linelist_covid_task import build_load_linelist_covid_task
from includes.reporting.load_linelist_FACTART_task import build_load_linelist_FACTART_task
from includes.reporting.load_linelist_hts_risk_categorization_and_test_results_task import build_load_linelist_hts_risk_categorization_and_test_results_task
from includes.reporting.load_linelist_otz_eligibility_and_enrollments_task import build_load_linelist_otz_eligibility_and_enrollments_task
from includes.reporting.load_linelist_otz_task import build_load_linelist_otz_task
from includes.reporting.load_linelist_ovc_eligibility_and_enrollment_task import build_load_linelist_ovc_eligibility_and_enrollment_task
from includes.reporting.load_linelist_ovc_enrollment_task import build_load_linelist_ovc_enrollment_task
from includes.reporting.load_linelist_prep_task import build_load_linelist_Prep_task
from includes.reporting.load_linelist_trans_hts_task import build_load_linelist_trans_hts_task
from includes.reporting.load_linelist_trans_pns_task import build_load_linelist_trans_pns_task
from includes.reporting.load_linelist_viralload_task import build_load_linelist_viralload_task
from includes.reporting.load_linelist_vl_non_suppressed_task import build_load_linelist_vl_non_suppressed_task
from includes.reporting.load_linelist_pbfw_task import build_load_linelist_pbfw_task
from includes.reporting.load_linelist_iit_risk_scores_task import build_load_linelist_iit_risk_scores_task
from includes.reporting.load_linelist_heis_task import build_load_linelist_heis_task
from includes.reporting.load_linelist_hts_eligibility_task import build_load_linelist_hts_eligibility_task


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

dag = DAG(dag_id='reporting_tables_etl_dag',
          schedule_interval=None,
          default_args=default_args,
          )

load_all_emr_sites = build_load_all_emr_sites_task(dag = dag)
load_linelist_adverse_events = build_load_linelist_adverse_events_task(dag = dag)
load_linelist_appointments = build_load_linelist_appointments_task(dag = dag)
load_linelist_covid = build_load_linelist_covid_task(dag = dag)
load_linelist_FACTART = build_load_linelist_FACTART_task(dag = dag)
load_linelist_hts_risk_categorization_and_test_results = build_load_linelist_hts_risk_categorization_and_test_results_task(dag = dag)
load_linelist_otz_eligibility_and_enrollments = build_load_linelist_otz_eligibility_and_enrollments_task(dag = dag)
load_linelist_otz = build_load_linelist_otz_task(dag = dag)
load_linelist_ovc_eligibility_and_enrollment = build_load_linelist_ovc_eligibility_and_enrollment_task(dag = dag)
load_linelist_ovc_enrollment = build_load_linelist_ovc_enrollment_task(dag = dag)
load_linelist_Prep = build_load_linelist_Prep_task(dag = dag)
load_linelist_trans_hts = build_load_linelist_trans_hts_task(dag = dag)
load_linelist_trans_pns = build_load_linelist_trans_pns_task(dag = dag)
load_linelist_viralload = build_load_linelist_viralload_task(dag = dag)
load_linelist_heis = build_load_linelist_heis_task(dag = dag)
load_linelist_iit_risk_scores = build_load_linelist_iit_risk_scores_task(dag = dag)
load_linelist_PBFW = build_load_linelist_pbfw_task(dag = dag)
load_linelist_vl_non_suppressed = build_load_linelist_vl_non_suppressed_task(dag = dag)
load_linelist_hts_eligibility = build_load_linelist_hts_eligibility_task(dag = dag)

reporting_aggregates_etl_trigger = TriggerDagRunOperator(
    task_id="trigger_reporting_aggregates_etl",
    trigger_dag_id = "reporting_aggregate_tables_etl_dag",
    dag=dag
)


