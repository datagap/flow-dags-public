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

harUrl = Variable.get("har_prop_url")

with DAG(
    dag_id='har-property-full-replication',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['har', 'replication'],
) as dag:

    start = DummyOperator(task_id='start')

    trigger = TriggerDagRunOperator(
        task_id="trigger_index",
        trigger_dag_id="har-property-full-index"
    )

    section = SubDagOperator(
        task_id='section',
        subdag=subdag('har-property-full-replication', 'section', args)
    )

    years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018","2019", "2020", "2021"]

    for year in years:
        url = harUrl + ' {y}'.format(y=year)
        
        task = KubernetesPodOperator(namespace='data',
                    image="datagap/dataloader:latest",
                    image_pull_policy='Always',
                    cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'har-{year}'".format(link=url,year=year)],
                    task_id="load-property-full-task-" + str(year),
                    name="load-property-full-task-" + str(year),
                    volumes=[volume],
                    volume_mounts=[volume_mount],
                    is_delete_operator_pod=True,
                    get_logs=True
                )

        start >> section >> task >> trigger
    