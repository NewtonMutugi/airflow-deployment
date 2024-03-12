from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from includes.load_fact_art_history_task import build_art_history_fact
from includes.load_fact_art_task import build_art_fact
from includes.load_fact_covid_task import build_covid_fact
from includes.load_fact_hts_client_linkages_task import build_load_fact_hts_client_linkages
from includes.load_fact_hts_client_tests_task import build_load_fact_hts_client_tests
from includes.load_fact_hts_client_tracing_task import build_load_fact_hts_client_tracing
from includes.load_fact_hts_eligibility_task import build_load_fact_hts_eligibility
from includes.load_fact_hts_partner_notification_task import build_load_fact_hts_partner_notification
from includes.load_fact_hts_partner_tracing_task import build_load_fact_hts_partner_tracing
from includes.load_fact_hts_test_kits_task import build_load_fact_hts_test_kits
from includes.load_fact_latest_obs_task import build_latest_obs_fact
from includes.load_fact_otz_task import build_otz_fact
from includes.load_fact_ovc_task import build_ovc_fact
from includes.load_fact_adverse_events_task import build_load_fact_adverse_events
from includes.load_fact_cd4_task import build_load_fact_cd4
from includes.load_fact_defaulter_tracing_task import build_load_fact_defaulter_tracing
from includes.load_fact_manifest_task import build_load_fact_manifest
from includes.load_fact_tpt_task import build_load_fact_tpt
from includes.load_fact_viral_load_task import build_load_fact_viral_load
from includes.load_fact_patient_exits_task import build_load_fact_patient_exits
from includes.load_fact_prep_assessments_task import build_load_fact_prep_assessments
from includes.load_fact_prep_discontinuation_task import build_load_fact_prep_discontinuation
from includes.load_fact_prep_refills_task import build_load_fact_prep_refills
from includes.load_fact_prep_visits_task import build_load_fact_prep_visits
from includes.load_fact_appointment_task import build_appointment_fact
from includes.load_fact_hei_task import build_load_fact_hei
from includes.load_fact_hts_pos_concordance import build_load_fact_hts_pos_concordance
from includes.load_fact_NCDs_task import build_load_fact_NCDs
from includes.load_fact_ushauri_appointments_task import build_load_fact_ushauri_appointments
from includes.load_fact_txcurr_concordance_task import build_load_fact_txcurr_concordance
from includes.load_fact_iit_risk_scores_task import build_load_fact_iit_risk_scores
from includes.load_fact_pbfw_task import build_load_fact_pbfw


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

spark_default_numpartitions = Variable.get("SPARK_DEFAULT_NUMPARTITIONS")
spark_dwapicentral_url = Variable.get("SPARK_DWAPICENTRAL_URL")
spark_dwapicentral_driver = Variable.get("SPARK_DWAPICENTRAL_DRIVER")
spark_dwapicentral_user = Variable.get("SPARK_DWAPICENTRAL_USER")
spark_dwapicentral_password = Variable.get("SPARK_DWAPICENTRAL_PASSWORD")
spark_ods_numpartitions = Variable.get("SPARK_ODS_NUMPARTITIONS")
spark_ods_url = Variable.get("SPARK_ODS_URL")
spark_ods_driver = Variable.get("SPARK_ODS_DRIVER")
spark_ods_user = Variable.get("SPARK_ODS_USER")
spark_ods_password = Variable.get("SPARK_ODS_PASSWORD")
spark_edw_url = Variable.get("SPARK_EDW_URL")
spark_edw_driver = Variable.get("SPARK_EDW_DRIVER")
spark_edw_user = Variable.get("SPARK_EDW_USER")
spark_edw_password = Variable.get("SPARK_EDW_PASSWORD")
spark_historical_url = Variable.get("SPARK_HISTORICAL_URL")
spark_historical_driver = Variable.get("SPARK_HISTORICAL_DRIVER")
spark_historical_user = Variable.get("SPARK_HISTORICAL_USER")
spark_historical_password = Variable.get("SPARK_HISTORICAL_PASSWORD")
spark_dim_agency_table = Variable.get("SPARK_DIM_AGENCY_TABLE")
spark_dim_age_group_table = Variable.get("SPARK_DIM_AGE_GROUP_TABLE")
spark_dim_art_outcome_table = Variable.get("SPARK_DIM_ART_OUTCOME_TABLE")
spark_dim_date_table = Variable.get("SPARK_DIM_DATE_TABLE")
spark_dim_facility_table = Variable.get("SPARK_DIM_FACILITY_TABLE")
spark_dim_partner_table = Variable.get("SPARK_DIM_PARTNER_TABLE")
spark_dim_patient_table = Variable.get("SPARK_DIM_PATIENT_TABLE")
spark_dim_drug_table = Variable.get("SPARK_DIM_DRUG_TABLE")
spark_dim_differentiated_care_table = Variable.get("SPARK_DIM_DIFFERENTIATED_CARE_TABLE")
spark_dim_relationship_with_patient_table = Variable.get("SPARK_DIM_RELATIONSHIP_WITH_PATIENT_TABLE")
spark_dim_regimen_line_table = Variable.get("SPARK_DIM_REGIMEN_LINE_TABLE")
spark_historical_art_outcome_base_table = Variable.get("SPARK_HISTORICAL_ART_OUTCOME_BASE_TABLE")
spark_fact_art_history_table = Variable.get("SPARK_FACT_ART_HISTORY_TABLE")
spark_fact_art_table = Variable.get("SPARK_FACT_ART_TABLE")
spark_fact_otz_table = Variable.get("SPARK_FACT_OTZ_TABLE")
spark_fact_ovc_table = Variable.get("SPARK_FACT_OVC_TABLE")
spark_fact_latest_obs_table = Variable.get("SPARK_FACT_LATEST_OBS_TABLE")
spark_fact_covid_table = Variable.get("SPARK_FACT_COVID_TABLE")
spark_intermediate_art_outcomes_table = Variable.get("SPARK_INTERMEDIATE_ART_OUTCOMES_TABLE")


default_conf = {
    "connection_id": "spark_standalone",
    "spark_app_home":spark_app_home,
    "default_spark_total_executor_cores": default_spark_total_executor_cores,
    "default_spark_executor_cores": default_spark_executor_cores,
    "default_spark_executor_memory": default_spark_executor_memory,
    "default_spark_driver_memory": default_spark_driver_memory,
    "spark.default.numpartitions":spark_default_numpartitions,
    "spark.driver.port": spark_driver_port,
    "spark.driver.blockManager.port": spark_driver_block_manager_port,
    "spark.driver.host": spark_driver_host,
    "spark.driver.bindAddress": spark_driver_bind_address,
    "spark.dwapicentral.url": spark_dwapicentral_url,
    "spark.dwapicentral.driver": spark_dwapicentral_driver,
    "spark.dwapicentral.user": spark_dwapicentral_user,
    "spark.dwapicentral.password": spark_dwapicentral_password,   
    "spark.ods.numpartitions":spark_ods_numpartitions, 
    "spark.ods.url": spark_ods_url,
    "spark.ods.driver": spark_ods_driver,
    "spark.ods.user": spark_ods_user,
    "spark.ods.password": spark_ods_password,
    "spark.edw.url": spark_edw_url,
    "spark.edw.driver": spark_edw_driver,
    "spark.edw.user": spark_edw_user,
    "spark.edw.password": spark_edw_password,
    "spark.Historical.url": spark_historical_url,
    "spark.Historical.driver": spark_historical_driver,
    "spark.Historical.user": spark_historical_user,
    "spark.Historical.password": spark_historical_password,
    "spark.dimAgency.dbtable": spark_dim_agency_table,
    "spark.dimARTOutcome.dbtable": spark_dim_art_outcome_table,
    "spark.dimAgeGroup.dbtable": spark_dim_age_group_table,
    "spark.dimDate.dbtable": spark_dim_date_table,
    "spark.dimFacility.dbtable": spark_dim_facility_table,
    "spark.dimPartner.dbtable": spark_dim_partner_table,
    "spark.dimPatient.dbtable": spark_dim_patient_table,
    "spark.dimDifferentiatedCare.dbtable":spark_dim_differentiated_care_table,
    "spark.dimRegimenLine.dbtable": spark_dim_regimen_line_table,
    "spark.dimRelationshipWithPatient.dbtable": spark_dim_relationship_with_patient_table,
    "spark.dimDrug.dbtable": spark_dim_drug_table,
    "spark.historicalArtOutcomeBase.dbtable": spark_historical_art_outcome_base_table,
    "spark.factArtHistory.dbtable": spark_fact_art_history_table,
    "spark.factArt.dbtable": spark_fact_art_table,
    "spark.factOtz.dbtable":spark_fact_otz_table,
    "spark.factOvc.dbtable": spark_fact_ovc_table,
    "spark.factLatestObs.dbtable": spark_fact_latest_obs_table,
    "spark.factCovid.dbtable": spark_fact_covid_table,
    "spark.intermediateArtOutcomes.dbtable": spark_intermediate_art_outcomes_table,
    "spark.sql.autoBroadcastJoinThreshold":-1
}

default_args = {
    'owner': 'kenyahmis',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 25, tzinfo=local_tz),
    'email': ['paul.nthusi@thepalladiumgroup.com','charles.bett@thepalladiumgroup.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}
dag = DAG(dag_id='edw_facts_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval=None)

art_history = build_art_history_fact(dag = dag, default_conf = default_conf)
art_fact = build_art_fact(dag = dag, default_conf = default_conf)
covid_fact = build_covid_fact(dag = dag, default_conf = default_conf)
load_fact_hts_client_linkages = build_load_fact_hts_client_linkages(dag = dag, default_conf = default_conf)
load_fact_hts_client_tests = build_load_fact_hts_client_tests(dag = dag, default_conf = default_conf)
load_fact_hts_client_tracing = build_load_fact_hts_client_tracing(dag = dag, default_conf = default_conf)
load_fact_hts_eligibility = build_load_fact_hts_eligibility(dag = dag, default_conf = default_conf)
load_fact_hts_partner_notification = build_load_fact_hts_partner_notification(dag = dag, default_conf = default_conf)
load_fact_hts_partner_tracing = build_load_fact_hts_partner_tracing(dag = dag, default_conf = default_conf)
load_fact_hts_test_kits = build_load_fact_hts_test_kits(dag = dag, default_conf = default_conf)
load_fact_latest_obs = build_latest_obs_fact(dag = dag, default_conf = default_conf)
load_fact_otz = build_otz_fact(dag = dag, default_conf = default_conf)
load_fact_ovc = build_ovc_fact(dag = dag, default_conf = default_conf)
load_fact_adverse_events = build_load_fact_adverse_events(dag = dag, default_conf = default_conf)
load_fact_cd4 = build_load_fact_cd4(dag = dag, default_conf = default_conf)
load_fact_defaulter_tracing = build_load_fact_defaulter_tracing(dag = dag, default_conf = default_conf)
load_fact_prep_assessments = build_load_fact_prep_assessments(dag = dag, default_conf = default_conf)
load_fact_manifest = build_load_fact_manifest(dag = dag, default_conf = default_conf)
load_fact_tpt = build_load_fact_tpt(dag = dag, default_conf = default_conf)
load_fact_viral_load = build_load_fact_viral_load(dag = dag, default_conf = default_conf)
load_fact_patient_exits = build_load_fact_patient_exits(dag = dag, default_conf = default_conf)
load_fact_prep_discontinuation = build_load_fact_prep_discontinuation(dag = dag, default_conf = default_conf)
load_fact_prep_refills = build_load_fact_prep_refills(dag = dag, default_conf = default_conf)
load_fact_prep_visits = build_load_fact_prep_visits(dag = dag, default_conf = default_conf)
load_fact_appointment = build_appointment_fact(dag = dag, default_conf = default_conf)
load_fact_hei = build_load_fact_hei(dag = dag, default_conf = default_conf)
load_fact_hts_pos_concordance = build_load_fact_hts_pos_concordance(dag = dag, default_conf = default_conf)
load_fact_ncds = build_load_fact_NCDs(dag = dag, default_conf = default_conf)
#load_fact_ushauri_appointments = build_load_fact_ushauri_appointments(dag = dag, default_conf = default_conf)
load_fact_txcurr_concordance = build_load_fact_txcurr_concordance(dag = dag, default_conf = default_conf)
load_fact_iit_risk_scores = build_load_fact_iit_risk_scores(dag = dag, default_conf = default_conf)
load_fact_pbfw = build_load_fact_pbfw(dag = dag, default_conf = default_conf)




art_history >> art_fact >> covid_fact >> load_fact_hts_client_linkages >> load_fact_hts_client_tests
load_fact_hts_client_tests >> load_fact_hts_client_tracing >> load_fact_hts_eligibility >> load_fact_hts_partner_notification
load_fact_hts_partner_notification >> load_fact_hts_partner_tracing >> load_fact_hts_test_kits
load_fact_hts_test_kits >> load_fact_latest_obs >> load_fact_otz >> load_fact_ovc >> load_fact_adverse_events
load_fact_adverse_events >> load_fact_cd4 >> load_fact_defaulter_tracing >> load_fact_prep_assessments
load_fact_prep_assessments >> load_fact_manifest >> load_fact_tpt >> load_fact_viral_load
load_fact_viral_load >> load_fact_patient_exits >> load_fact_prep_discontinuation >> load_fact_prep_refills
load_fact_prep_refills >> load_fact_prep_visits >> load_fact_appointment >> load_fact_hei >> load_fact_hts_pos_concordance
load_fact_hts_pos_concordance >>load_fact_ncds >> load_fact_txcurr_concordance>> load_fact_iit_risk_scores>>load_fact_pbfw
# waiting on ODS Table
# load_fact_ushauri_appointments 