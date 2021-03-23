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

login_url = Variable.get("ntreis_login_url")
rets_type = Variable.get("ntreis_rets_type")
search_limit = Variable.get("ntreis_search_limit")
password = Variable.get("ntreis_password")
user_agent = Variable.get("ntreis_user_agent")
working_dir = Variable.get("ntreis_working_dir")
server_version = Variable.get("ntreis_server_version")
username = Variable.get("ntreis_username")

soldTemplateUrl = Variable.get("ntreis_prop_delta_sold_index_url")
soldPropDataSource = Variable.get("ntreis_prop_sold_datasource")

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
  result['spec']['dataSchema']['granularitySpec']['intervals'] = intervals

  return result

def createIndexSpec(templateContent, dataSource, intervals):
  baseDir = '/var/shared-data/ntreis-delta'
  template = replace(templateContent, baseDir, dataSource, intervals)

  return template

with DAG(
    dag_id='ntreis-property-delta',
    default_args=default_args,
    schedule_interval="0 8 * * *",
    start_date=days_ago(2),
    tags=['ntreis', 'delta'],
) as dag:

    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    today = datetime.now().strftime('%Y-%m-%d')
    # index interval in format 2020-01-01/2020-01-02
    intervals = '{yesterday}/{today}'.format(yesterday=yesterday, today=today)

    query = 'StatusChangeTimestamp={d}T00:00:00-{d}T23:59:59'.format(d=yesterday)
    # sold
    soldTemplateContent = downloadTemplate(soldTemplateUrl)
    soldIndexSpec = createIndexSpec(soldTemplateContent, soldPropDataSource, intervals)

    start = DummyOperator(task_id='start')

    load = KubernetesPodOperator(namespace='data',
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

    sold = SimpleHttpOperator(
                task_id='submit-property-index-' + yesterday,
                method='POST',
                http_conn_id='druid-cluster',
                endpoint='druid/indexer/v1/task',
                headers={"Content-Type": "application/json"},
                data=json.dumps(soldIndexSpec),
                response_check=lambda response: True if response.status_code == 200 else False)
            
    start >> load >> sold
    