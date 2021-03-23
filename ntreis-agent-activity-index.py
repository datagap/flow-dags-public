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

listUrl = Variable.get("ntreis_agent_list_activity_index_url")
buyerUrl = Variable.get("ntreis_agent_buyer_activity_index_url")
target = Variable.get("ntreis_agent_activity_datasource")
source = Variable.get("ntreis_prop_sold_datasource")

def download(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def createIndexSpec(jsonContent, source, target):
  
  result = json.loads(jsonContent)

  result['spec']['ioConfig']['inputSource']['dataSource'] = source
  result['spec']['dataSchema']['dataSource'] = target

  return result

with DAG(
    dag_id='ntreis-agent-activity-index',
    default_args=default_args,
    schedule_interval="0 9 * * *",
    start_date=days_ago(2),
    tags=['ntreis', 'index'],
) as dag:
    
    # download list activity index template
    listTemplateContent = download(listUrl)
    # download buyer activity index template
    buyerTemplateContent = download(buyerUrl)

    # create list index spec
    listIndexSpec = createIndexSpec(listTemplateContent, source, target)
    # create buyer index spec
    buyerIndexSpec = createIndexSpec(buyerTemplateContent, source, target)

    start = DummyOperator(task_id='start')

    wait = BashOperator(
                task_id='wait-for-30m',
                bash_command="sleep 30m")

    listTask = SimpleHttpOperator(
            task_id='submit-agent-list-index',
            method='POST',
            http_conn_id='druid-cluster',
            endpoint='druid/indexer/v1/task',
            headers={"Content-Type": "application/json"},
            data=json.dumps(listIndexSpec),
            response_check=lambda response: True if response.status_code == 200 else False)
    
    buyerTask = SimpleHttpOperator(
            task_id='submit-agent-buyer-index',
            method='POST',
            http_conn_id='druid-cluster',
            endpoint='druid/indexer/v1/task',
            headers={"Content-Type": "application/json"},
            data=json.dumps(buyerIndexSpec),
            response_check=lambda response: True if response.status_code == 200 else False)


    start >> listTask >> wait >> buyerTask
            
        

    
    