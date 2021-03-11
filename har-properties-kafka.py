from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'datagap'
}

dag = DAG(
    'har-properties-kafka', 
    default_args=default_args, 
    schedule_interval=None,
    start_date=datetime.now()
)


start = DummyOperator(task_id='start', dag=dag)

# current year
year = year = datetime.now().year

# ingestion 10 years
for i in range(11):
    url = 'https://api.bridgedataoutput.com/api/v2/OData/har/Property/replication?access_token=c28535e677fb3fdf78253a99d3c5c1b2&$filter=year(ModificationTimestamp) eq {y}'.format(y=year)
    
    task = KubernetesPodOperator(namespace='ingestion',
                image="datagap/dataingestion",
                image_pull_policy='IfNotPresent',
                cmds=["sh","-c", "dotnet DataIngestion.dll 'kafka-cp-kafka-headless.kafka.svc.cluster.local:9092' 'har-properties-raw' '{link}'".format(link=url)],
                annotations={'chaos.alpha.kubernetes.io/enabled': 'true'},
                task_id="deploy-ingestion-pod-task-" + str(year),
                name="har-properties-full-pod-" + str(year),
                get_logs=True,
                dag=dag
            )

    start >> task
    # another year
    year = year - 1
    