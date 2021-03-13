from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago
import urllib.request
import json
from airflow.models import Variable
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'datagap'
}

templateUrl = Variable.get("druid_file_index_url")
harPropDataSource = Variable.get("har_prop_datasource")

def download(templateUrl):
  request = urllib.request.urlopen(templateUrl)
  response = request.read().decode('utf-8')

  return response

def replace(jsonContent, baseDir, dataSource):
  
  result = json.loads(jsonContent)

  result['spec']['ioConfig']['inputSource']['baseDir'] = baseDir
  result['spec']['dataSchema']['dataSource'] = dataSource

  return result

with DAG(
    dag_id='har-properties-full-index',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(2),
    tags=['har'],
) as dag:

    start = DummyOperator(task_id='start')

    wait = BashOperator(
        task_id='wait_20_mins',
        bash_command="sleep 20m"
    )

    templateContent = download(templateUrl)

    years = ["2010", "2011", "2012", "2013", "2014", "2015", "2016", "2017", "2018","2019", "2020", "2021"]

    for year in years:
        url = 'https://api.bridgedataoutput.com/api/v2/OData/har/Property/replication?access_token=c28535e677fb3fdf78253a99d3c5c1b2&$filter=year(ModificationTimestamp) eq {y}'.format(y=year)
        
        baseDir = 'har-{year}'.format(year=year)

        template = replace(templateContent, baseDir, harPropDataSource)


        start >> task >> wait
        # sleep for 20 mins
    