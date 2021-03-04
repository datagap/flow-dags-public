from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'datagap',
    'depends_on_past': False,
    'start_date': days_ago(-1),
    'email': ['truong@datagap.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'har-properties-daily', default_args=default_args, schedule_interval='@daily')


start = DummyOperator(task_id='start', dag=dag)

yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

url = 'https://api.bridgedataoutput.com/api/v2/OData/har/Property/replication?access_token=c28535e677fb3fdf78253a99d3c5c1b2&$filter=date(ModificationTimestamp) eq {d}'.format(d=yesterday)

task = KubernetesPodOperator(namespace='ingestion',
            image="datagap/dataingestion",
            image_pull_policy='IfNotPresent',
            cmds=["sh","-c", "dotnet DataIngestion.dll 'dip-cluster-kafka-bootstrap.stream.svc.cluster.local:9092' 'har-properties-topic' '{link}'".format(link=url)],
            annotations={'chaos.alpha.kubernetes.io/enabled': 'true'},
            task_id="deploy-ingestion-pod-task-" + yesterday,
            name="har-properties-daily-pod-" + yesterday,
            get_logs=True,
            dag=dag
        )

start >> task
    