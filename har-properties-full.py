from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'datagap',
    'depends_on_past': False,
    'start_date': datetime(2021, 3, 8),
    'email': ['truong@datagap.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'har-properties-full', default_args=default_args, schedule_interval='@once')


start = DummyOperator(task_id='start', dag=dag)

# current year
year = year = datetime.now().year

# ingestion 10 years
for i in range(11):
    url = 'https://api.bridgedataoutput.com/api/v2/OData/har/Property/replication?access_token=c28535e677fb3fdf78253a99d3c5c1b2&$filter=year(ModificationTimestamp) eq {y}'.format(y=year)
    
    task = KubernetesPodOperator(namespace='ingestion',
                image="datagap/dataingestion",
                image_pull_policy='IfNotPresent',
                cmds=["sh","-c", "dotnet DataIngestion.dll 'dip-cluster-kafka-bootstrap.stream.svc.cluster.local:9092' 'har-properties-topic' '{link}'".format(link=url)],
                annotations={'chaos.alpha.kubernetes.io/enabled': 'true'},
                task_id="deploy-ingestion-pod-task-" + str(year),
                name="har-properties-full-pod-" + str(year),
                get_logs=True,
                dag=dag
            )

    start >> task
    # another year
    year = year - 1
    