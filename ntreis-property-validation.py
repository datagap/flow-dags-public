from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator
from kubernetes.client import models as k8s
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.bash import BashOperator
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
templateUrl = Variable.get("ntreis_druid_validation_index_url")
ntreisPropDatasource = Variable.get("ntreis_prop_sold_datasource")
validationDatasource = Variable.get("validation_datasource")

login_url = Variable.get("ntreis_login_url")
rets_type = Variable.get("ntreis_rets_type")
search_limit = Variable.get("ntreis_search_limit")
password = Variable.get("ntreis_password")
user_agent = Variable.get("ntreis_user_agent")
working_dir = Variable.get("ntreis_working_dir")
server_version = Variable.get("ntreis_server_version")
username = Variable.get("ntreis_username")

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
  result['spec']['dataSchema']['transformSpec']['transforms'][0]['expression'] = systemSource

  return result

def createIndexSpec(templateContent, dataSource, intervals, systemSource):
  baseDir = '/var/shared-data/ntreis-validation'
  template = replace(templateContent, baseDir, dataSource, intervals, systemSource)

  return template

with DAG(
    dag_id='ntreis-property-validation',
    default_args=default_args,
    schedule_interval="0 10 * * *",
    start_date=days_ago(2),
    tags=['ntreis', 'validation'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    # index interval in format 2020-01-01/2020-01-02
    intervals = '{yesterday}/{today}'.format(yesterday=yesterday, today=today)

    query = 'StatusChangeTimestamp={d}T00:00:00-{d}T23:59:59'.format(d=yesterday)

    druidJson = {
      "query":"SELECT __time, ListOfficeName, MLS FROM \"{datasource}\" WHERE \"__time\" BETWEEN TIMESTAMP '{yesterday}' AND TIMESTAMP '{today}'".format(datasource=ntreisPropDatasource, yesterday=yesterday, today=today)
    }
    druidQuery = json.dumps(druidJson)

    indexTemplate = downloadTemplate(templateUrl)

    druidIndexSpec = createIndexSpec(indexTemplate, validationDatasource, intervals, 'nvl("dummyCol1", \'Druid\')')
    mlsIndexSpec = createIndexSpec(indexTemplate, validationDatasource, intervals, 'nvl("dummyCol1", \'MLS\')')

    start = DummyOperator(task_id='start')

    wait = BashOperator(
                task_id='wait-for-15m',
                bash_command="sleep 15m")    

    loadDruid = KubernetesPodOperator(namespace='data',
                image="truongretell/druiddataloader:latest",
                image_pull_policy='Always',
                cmds=["sh","-c", "dotnet DruidDataLoader.dll '{link}' '/shared-data' 'ntreis-validation' '{yesterday}' '{query}'".format(link=druidUrl, yesterday=yesterday, query=druidQuery)],
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
                    image="datagap/retsconnector:latest",
                    image_pull_policy='Always',
                    cmds=["sh","-c", "dotnet RetsConnector.dll '{query}'".format(query=query)],
                    task_id="load-property-delta-task-" + str(yesterday),
                    name="load-property-delta-task-" + str(yesterday),
                    volumes=[volume],
                    volume_mounts=[volume_mount],
                    is_delete_operator_pod=True,
                    get_logs=True,
                    env_vars={
                        'RETS_LOGIN_URL': login_url,
                        'RETS_TYPE': rets_type,
                        'RETS_SEARCH_LIMIT': search_limit,
                        'RETS_PASSWORD': password,
                        'RETS_USER_AGENT': user_agent,
                        'WORKING_DIR': working_dir,
                        'DIR_NAME': 'ntreis-delta',
                        'RETS_SERVER_VERSION': server_version,
                        'RETS_USERNAME': username}
                )  

    indexMls = SimpleHttpOperator(
                task_id='submit-mls-validation-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(mlsIndexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            

    start >> loadMls >> indexMls >> wait >> loadDruid >> indexDruid
    



