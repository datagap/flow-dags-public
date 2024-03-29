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

templateUrl = Variable.get("permit_data_austin_batch_index_url")
permitDataSource = Variable.get("permit_datasource")

def downloadTemplate(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, dataSource, interval, market):
  
  result = json.loads(jsonContent)
  # base data source
  result['spec']['ioConfig']['inputSource']['dataSource'] = dataSource
  # datasource
  result['spec']['dataSchema']['dataSource'] = dataSource
  # added Market column
  result['spec']['dataSchema']['transformSpec']['transforms'][0]['expression'] = market
  # granularity intervals
  result['spec']['dataSchema']['granularitySpec']['intervals'] = [interval]

  return result

def createIndexSpec(templateContent, dataSource, interval, market):
  template = replace(templateContent, dataSource, interval, market)

  return template

with DAG(
    dag_id='permit-data-austin-batch',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['permit'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')

    templateContent = downloadTemplate(templateUrl)
    interval = '2020-01-01/{yesterday}'.format(yesterday=yesterday)

    indexSpec = createIndexSpec(templateContent, permitDataSource, interval, 'nvl("dummyCol1", \'Austin\')')

    start = DummyOperator(task_id='start')
    index = SimpleHttpOperator(
                task_id='permit-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(indexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            

    start >> index
    