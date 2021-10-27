from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import urllib.request
import json
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
import time
from airflow.operators.http_operator import SimpleHttpOperator

default_args = {
    'owner': 'datagap'
}

templateUrl = Variable.get("sabor_prop_sold_index_url")
saborPropDataSource = Variable.get("sabor_prop_sold_datasource")

def download(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, baseDir, dataSource):
  
  result = json.loads(jsonContent)

  result['spec']['ioConfig']['inputSource']['baseDir'] = baseDir
  result['spec']['dataSchema']['dataSource'] = dataSource

  return result

def createIndexSpec(templateContent, year, propDataSource):
  baseDir = '/var/shared-data/sabor-{year}'.format(year=year)
  template = replace(templateContent, baseDir, propDataSource)

  return template

with DAG(
    dag_id='sabor-property-sold-index',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['sabor', 'index'],
) as dag:

    start = DummyOperator(task_id='start')

    templateContent = download(templateUrl)


    years = ["2014", "2015", "2016", "2017", "2018","2019", "2020", "2021"]
    tasks = []
    index = 0

    for year in years:
        indexSpec = createIndexSpec(templateContent, year, saborPropDataSource)

        wait = BashOperator(
                task_id='wait-for-10m-' + year,
                bash_command="sleep 10m")

        tasks.append(
            SimpleHttpOperator(
                task_id='submit-index-' + year,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(indexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            )

        # sequential, wait in between
        if index > 0:
            tasks[index-1] >> wait >> tasks[index]

        index = index + 1

    # start with first task
    start >> tasks[0]
    