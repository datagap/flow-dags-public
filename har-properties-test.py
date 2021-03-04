from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'datagap',
    'depends_on_past': False,
    'start_date': datetime.utcnow(),
    'email': ['truong@datagap.io'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kube-ingestion-test', default_args=default_args, schedule_interval=None)


start = DummyOperator(task_id='start', dag=dag)

for i in range(5):
    url = 'https://api.bridgedataoutput.com/api/v2/OData/har/Property/replication?access_token=c28535e677fb3fdf78253a99d3c5c1b2&$filter=date(ModificationTimestamp) eq 2020-02-0{index}'.format(index=i+1)
    
    task = KubernetesPodOperator(namespace='ingestion',
                image="datagap/dataingestion",
                image_pull_policy='IfNotPresent',
                cmds=["sh","-c", "dotnet DataIngestion.dll 'dip-cluster-kafka-bootstrap.stream.svc.cluster.local:9092' 'har-properties-topic' '{link}'".format(link=url)],
                annotations={'chaos.alpha.kubernetes.io/enabled': 'true'},
                # name="har-properties-test",
                task_id="create-ingestion-pod-task-" + str(i+1),
                name="create-ingestion-pod-task-" + str(i+1),
                get_logs=True,
                dag=dag
            )

    start >> task