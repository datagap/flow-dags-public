from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from airflow.models import Variable

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

harUrl = Variable.get("har_prop_delta_url")

with DAG(
    dag_id='har-property-delta',
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=days_ago(2),
    tags=['har', 'delta'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    url = harUrl + ' {d}'.format(d=yesterday)

    start = DummyOperator(task_id='start')

    task = KubernetesPodOperator(namespace='data',
                image="datagap/dataloader:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'har-delta'".format(link=url)],
                task_id="deploy-har-delta-pod-task-" + str(yesterday),
                name="har-properties-delta-pod-" + str(yesterday),
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True
            )

    start >> task
    