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

actrisUrl = Variable.get("actris_prop_url")

with DAG(
    dag_id='actris-properties-full-replication',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['actris', 'replication'],
) as dag:

    start = DummyOperator(task_id='start')

    years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018","2019", "2020", "2021"]

    for year in years:
        url = actrisUrl + ' {y}'.format(y=year)
        
        task = KubernetesPodOperator(namespace='data',
                    image="datagap/dataloader:latest",
                    image_pull_policy='IfNotPresent',
                    cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'actris-{year}'".format(link=url,year=year)],
                    task_id="deploy-data-loader-pod-task-" + str(year),
                    name="actris-properties-loader-pod-" + str(year),
                    volumes=[volume],
                    volume_mounts=[volume_mount],
                    is_delete_operator_pod=True,
                    get_logs=True
                )

        start >> task
    