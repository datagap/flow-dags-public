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

basePath = Variable.get("permit_data_base_url")
templateUrl = Variable.get("permit_data_weekly_index_url")
permitDataSource = Variable.get("permit_datasource")

def downloadTemplate(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, dataSource, basePath, date, market):
  
  result = json.loads(jsonContent)
  result['spec']['ioConfig']['inputSource']['uris'] = [
    basePath + 'Dallas/' + date + '_Dallas%20Fort%20Worth.csv'
  ]
  # datasource
  result['spec']['dataSchema']['dataSource'] = dataSource
  result['spec']['dataSchema']['transformSpec']['transforms'][0]['expression'] = market

  return result

def createIndexSpec(templateContent, dataSource, basePath, date, market):
  template = replace(templateContent, dataSource, basePath, date, market)

  return template

with DAG(
    dag_id='permit-data-weekly',
    default_args=default_args,
    schedule_interval="0 0 * * 5", # Weekly at midnight on Friday
    start_date=days_ago(2),
    tags=['permit'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y%m%d')

    templateContent = downloadTemplate(templateUrl)
    indexSpec = createIndexSpec(templateContent, permitDataSource, basePath, yesterday, 'nvl("dummyCol1", \'Dallas\')')

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
    