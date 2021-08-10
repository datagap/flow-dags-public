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

druidUrl = Variable.get("druid_broker_url")
harUrl = Variable.get("har_prop_delta_url")
templateUrl = Variable.get("har_druid_validation_index_url")
harPropDatasource = Variable.get("har_prop_sold_datasource")
validationDatasource = Variable.get("validation_datasource")

def downloadTemplate(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, baseDir, dataSource, intervals, systemSource):
  
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
  # system source
  result['spec']['dataSchema']['transformSpec']['transforms']['expression'] = [systemSource]

  return result

def createIndexSpec(templateContent, dataSource, intervals, systemSource):
  baseDir = '/var/shared-data/har-validation'
  template = replace(templateContent, baseDir, dataSource, intervals, systemSource)

  return template

with DAG(
    dag_id='har-property-validation',
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=days_ago(2),
    tags=['har', 'validation'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    # index interval in format 2020-01-01/2020-01-02
    intervals = '{yesterday}/{today}'.format(yesterday=yesterday, today=today)

    druidJson = {
      "query":"SELECT ListingId, __time, ListOfficeName FROM \"{datasource}\" WHERE \"__time\" BETWEEN TIMESTAMP '{yesterday}' AND TIMESTAMP '{today}'".format(datasource=harPropDatasource, yesterday=yesterday, today=today)
    }
    druidQuery = json.dumps(druidJson)

    mlsUrl = harUrl + ' {d}'.format(d=yesterday)

    indexTemplate = downloadTemplate(templateUrl)

    druidIndexSpec = createIndexSpec(indexTemplate, validationDatasource, intervals, 'druid')
    mlsIndexSpec = createIndexSpec(indexTemplate, validationDatasource, intervals, 'mls')

    start = DummyOperator(task_id='start')

    loadDruid = KubernetesPodOperator(namespace='data',
                image="truongretell/druiddataloader:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet DruidDataLoader.dll '{link}' '/shared-data' 'har-validation' '{yesterday}' '{query}'".format(link=druidUrl, yesterday=yesterday, query=druidQuery)],
                task_id="load-property-sold-validation-task-" + str(yesterday),
                name="load-property-sold-validation-task-" + str(yesterday),
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True
            )

    indexDruid = SimpleHttpOperator(
                task_id='submit-druid-validation-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(druidIndexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)

    loadMls = KubernetesPodOperator(namespace='data',
                image="datagap/dataloader:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet DataLoader.dll '{link}' '/shared-data' 'har-validation'".format(link=mlsUrl)],
                task_id="load-mls-property-validation-task-" + str(yesterday),
                name="load-mls-property-validation-task-" + str(yesterday),
                volumes=[volume],
                volume_mounts=[volume_mount],
                is_delete_operator_pod=True,
                get_logs=True
            )

    indexMls = SimpleHttpOperator(
                task_id='submit-mls-validation-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(mlsIndexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            

    start >> loadDruid >> indexDruid >> loadMls >> indexMls
    