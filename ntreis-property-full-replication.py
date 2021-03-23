
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.subdag import SubDagOperator

default_args = {
    'owner': 'datagap'
}

volume = k8s.V1Volume(
    name='data-volume',
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name='shared-data-volume')
)

volume_mount = k8s.V1VolumeMount(
    name='data-volume', mount_path='/shared-data', sub_path=None, read_only=False
)

login_url = Variable.get("ntreis_login_url")
rets_type = Variable.get("ntreis_rets_type")
search_limit = Variable.get("ntreis_search_limit")
password = Variable.get("ntreis_password")
user_agent = Variable.get("ntreis_user_agent")
working_dir = Variable.get("ntreis_working_dir")
server_version = Variable.get("ntreis_server_version")
username = Variable.get("ntreis_username")

with DAG(
    dag_id='ntreis-property-full-replication',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['ntreis', 'replication'],
) as dag:

    start = DummyOperator(task_id='start')

    years = ["2016", "2017", "2018","2019", "2020", "2021"]
    tasks = []
    index = 0

    for year in years:
        # StatusChangeTimestamp|2020-01-01T00:00:00-2020-12-31T23:59:59
        query = 'StatusChangeTimestamp={y}-01-01T00:00:00-{y}-12-31T23:59:59'.format(y=year)
        
        tasks.append(
            KubernetesPodOperator(namespace='data',
                image="datagap/retsconnector:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet RetsConnector.dll '{query}'".format(query=query)],
                task_id="load-property-full-task-" + str(year),
                name="load-property-full-task-" + str(year),
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True,
                env_vars={
                    'RETS_LOGIN_URL': login_url,
                    'RETS_TYPE': rets_type,
                    'RETS_SEARCH_LIMIT': search_limit,
                    'RETS_PASSWORD': password,
                    'RETS_USER_AGENT': user_agent,
                    'WORKING_DIR': working_dir,
                    'DIR_NAME': 'ntreis-' + year,
                    'RETS_SERVER_VERSION': server_version,
                    'RETS_USERNAME': username})
        )
        
        if index > 0:
            tasks[index-1] >> tasks[index]

        index = index + 1
        

    # start with first task
    start >> tasks[0]
    