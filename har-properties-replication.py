from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.utils.dates import days_ago

DAG_NAME = 'har_properties_replication'

args = {
    'owner': 'datagap',
}

dag = DAG(
    dag_id=DAG_NAME,
    default_args=args,
    start_date=days_ago(2),
    schedule_interval='1 * * * *',
    tags=['har']
)

t1 = SimpleHttpOperator(
    task_id='get_druid_tasks',
    method='GET',
    endpoint='http://druid-druid-cluster-routers.data.svc.cluster.local/druid­/in­dex­er/­v1/­tasks',
    headers={"Content-Type": "application/json"},
    response_check=lambda response: True if len(response.json()) == 0 else False,
    dag=dag)

t1