from datetime import datetime, timedelta
from airflow.models import Variable
import pendulum
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from includes.load_hts_client_linkages_task import build_load_hts_client_linkages
from includes.load_hts_client_tests_task import build_load_hts_client_tests
from includes.load_hts_client_tracing_task  import build_load_hts_client_tracing
from includes.load_hts_clients_task import build_load_hts_clients
from includes.load_hts_eligibility_task import build_load_hts_eligibility
from includes.load_hts_partner_notification_services_task import build_load_hts_partner_notification_services
from includes.load_hts_partner_tracing_task import build_load_hts_partner_tracing
from includes.load_hts_test_kits_task import build_load_hts_test_kits

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

spark_htscentral_url = Variable.get("SPARK_HTSCENTRAL_URL")
spark_htscentral_driver = Variable.get("SPARK_HTSCENTRAL_DRIVER")
spark_htscentral_user = Variable.get("SPARK_HTSCENTRAL_USER")
spark_htscentral_password = Variable.get("SPARK_HTSCENTRAL_PASSWORD")
spark_htscentral_numpartitions = Variable.get("SPARK_HTSCENTRAL_NUMPARTITIONS")

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
    "spark.htscentral.url": spark_htscentral_url,
    "spark.htscentral.driver": spark_htscentral_driver,
    "spark.htscentral.user": spark_htscentral_user,
    "spark.htscentral.password": spark_htscentral_password,
    "spark.htscentral.numpartitions": spark_htscentral_numpartitions,
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
dag = DAG(dag_id='ods_hts_etl_dag',
          default_args=default_args,
          catchup=False,
          schedule_interval=None,
          )

load_hts_client_linkages = build_load_hts_client_linkages(dag=dag, default_conf = default_conf)
load_hts_client_tests = build_load_hts_client_tests(dag=dag, default_conf = default_conf)
load_hts_client_tracing = build_load_hts_client_tracing(dag=dag, default_conf = default_conf)
load_hts_clients = build_load_hts_clients(dag=dag, default_conf = default_conf)
load_hts_eligibility = build_load_hts_eligibility(dag=dag, default_conf = default_conf)
load_hts_partner_notification_services = build_load_hts_partner_notification_services(dag=dag, default_conf = default_conf)
load_hts_partner_tracing = build_load_hts_partner_tracing(dag=dag, default_conf = default_conf)
load_hts_test_kits = build_load_hts_test_kits(dag=dag, default_conf = default_conf)
ods_mnch_etl_trigger = TriggerDagRunOperator(
    task_id="trigger_ods_mnch_etl",
    trigger_dag_id = "ods_mnch_etl_dag",
    dag=dag
)

load_hts_client_linkages >> load_hts_client_tests >> load_hts_client_tracing >> load_hts_clients
load_hts_clients >> load_hts_eligibility >> load_hts_partner_notification_services >> load_hts_partner_tracing
load_hts_partner_tracing >> load_hts_test_kits >> ods_mnch_etl_trigger
