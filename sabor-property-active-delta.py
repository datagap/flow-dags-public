from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.subdag import SubDagOperator
import urllib.request
import json

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

saborTokenUrl = Variable.get("sabor_token_url")
saborDataUrl = Variable.get("sabor_prop_url")

saborClientId = Variable.get("sabor_client_id")
saborClientSecret = Variable.get("sabor_client_secret")
saborScope= Variable.get("sabor_scope")

templateUrl = Variable.get("sabor_prop_active_index_url")
saborPropDataSource = Variable.get("sabor_prop_active_datasource")

def download(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, baseDir, dataSource, interval):
  
  result = json.loads(jsonContent)

  result['spec']['ioConfig']['inputSource']['baseDir'] = baseDir
  result['spec']['dataSchema']['dataSource'] = dataSource
  # granularity intervals
  result['spec']['dataSchema']['granularitySpec']['intervals'] = [interval]

  return result

def createIndexSpec(templateContent, saborPropDataSource, interval):
  baseDir = '/var/shared-data/sabor-replication'
  template = replace(templateContent, baseDir, saborPropDataSource, interval)

  return template


with DAG(
    dag_id='sabor-property-active-delta',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['sabor', 'delta'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    # index interval in format 2020-01-01/2020-01-02
    intervals = '{yesterday}/{today}'.format(yesterday=yesterday, today=today)

    url = saborDataUrl + 'StatusChangeTimestamp ge {y}T00:00:00Z and StatusChangeTimestamp lt {t}T00:00:00Z'.format(y=yesterday, t=today)

    start = DummyOperator(task_id='start')

    templateContent = download(templateUrl)
    indexSpec = createIndexSpec(templateContent, saborPropDataSource, intervals)
        
    task = KubernetesPodOperator(namespace='data',
                image="truongretell/saboringestion:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet SaborIngestion.dll '{tokenUrl}' '{dataUrl}' '{clientId}' '{clientSecret}' '{scope}' '/shared-data' 'sabor-replication'".format(tokenUrl=saborTokenUrl,dataUrl=url,clientId=saborClientId,clientSecret=saborClientSecret,scope=saborScope)],
                task_id="load-property-full-task-sabor",
                name="load-property-full-task-sabor",
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True
            )

    index = SimpleHttpOperator(
                task_id='submit-property-index',
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(indexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)

    start >> task >> index
    