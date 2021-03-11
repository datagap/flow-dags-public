from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago

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

# current year
year = year = datetime.now().year

with DAG(
    dag_id='har-properties-loader',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['har'],
) as dag:

    start = DummyOperator(task_id='start')

    # ingestion 10 years
    for i in range(11):
        url = 'https://api.bridgedataoutput.com/api/v2/OData/har/Property/replication?access_token=c28535e677fb3fdf78253a99d3c5c1b2&$filter=year(ModificationTimestamp) eq {y}'.format(y=year)
        
        task = KubernetesPodOperator(namespace='data',
                    image="datagap/dataloader",
                    image_pull_policy='IfNotPresent',
                    cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'har-2021'".format(link=url)],
                    annotations={'chaos.alpha.kubernetes.io/enabled': 'true'},
                    task_id="deploy-data-loader-pod-task-" + str(year),
                    name="har-properties-loader-pod-" + str(year),
                    volumes=[volume],
                    volume_mounts=[volume_mount],
                    get_logs=True
                )

        start >> task
        # another year
        year = year - 1
    