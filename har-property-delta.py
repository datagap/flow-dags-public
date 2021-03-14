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

harUrl = Variable.get("har_prop_delta_url")
templateUrl = Variable.get("har_prop_index_url")
harPropDataSource = Variable.get("har_prop_datasource")

def downloadTemplate(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, baseDir, dataSource):
  
  result = json.loads(jsonContent)

  result['spec']['ioConfig']['inputSource']['baseDir'] = baseDir
  result['spec']['dataSchema']['dataSource'] = dataSource

  return result

def createIndexSpec(templateContent, harPropDataSource):
  baseDir = '/var/shared-data/har-delta'
  template = replace(templateContent, baseDir, harPropDataSource)

  return template

with DAG(
    dag_id='har-property-delta',
    default_args=default_args,
    schedule_interval="0 7 * * *",
    start_date=days_ago(2),
    tags=['har', 'delta'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    url = harUrl + ' {d}'.format(d=yesterday)

    templateContent = downloadTemplate(templateUrl)
    indexSpec = createIndexSpec(templateContent, harPropDataSource)

    start = DummyOperator(task_id='start')

    load = KubernetesPodOperator(namespace='data',
                image="datagap/dataloader:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'har-delta'".format(link=url)],
                task_id="load-property-delta-task-" + str(yesterday),
                name="load-property-delta-task-" + str(yesterday),
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True
            )

    index = SimpleHttpOperator(
                task_id='submit-property-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(indexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            

    start >> load >> index
    