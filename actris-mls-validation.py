from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.http_operator import SimpleHttpOperator
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

actrisUrl = Variable.get("actris_mls_validation_url")
templateUrl = Variable.get("actris_mls_validation_index_url")
actrisPropDataSource = Variable.get("actris_mls_validation_datasource")

def downloadTemplate(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, baseDir, dataSource, intervals):
  
  result = json.loads(jsonContent)

  # input intervals
  result['spec']['ioConfig']['inputSource']['delegates'][0]['dataSource'] = dataSource
  # base data source
  result['spec']['ioConfig']['inputSource']['delegates'][0]['interval'] = intervals
  # base directory
  result['spec']['ioConfig']['inputSource']['delegates'][1]['baseDir'] = baseDir
  # datasource
  result['spec']['dataSchema']['dataSource'] = dataSource
  # granularity intervals
  result['spec']['dataSchema']['granularitySpec']['intervals'] = [intervals]

  return result

def createIndexSpec(templateContent, dataSource, intervals):
  baseDir = '/var/shared-data/actris-mls-validation'
  template = replace(templateContent, baseDir, dataSource, intervals)

  return template

with DAG(
    dag_id='actris-mls-validation',
    default_args=default_args,
    schedule_interval="0 6 * * *",
    start_date=days_ago(2),
    tags=['har', 'mlsvalidation'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    # index interval in format 2020-01-01/2020-01-02
    intervals = '{yesterday}/{today}'.format(yesterday=yesterday, today=today)

    url = actrisUrl + ' {d}'.format(d=yesterday)

    templateContent = downloadTemplate(templateUrl)
    indexSpec = createIndexSpec(templateContent, actrisPropDataSource, intervals)

    start = DummyOperator(task_id='start')

    load = KubernetesPodOperator(namespace='data',
                image="datagap/dataloader:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'actris-mls-validation'".format(link=url)],
                task_id="load-mls-validation-task-" + str(yesterday),
                name="load-mls-validation-task-" + str(yesterday),
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True
            )

    index = SimpleHttpOperator(
                task_id='submit-mls-validation-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(indexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            

    start >> load >> index
    