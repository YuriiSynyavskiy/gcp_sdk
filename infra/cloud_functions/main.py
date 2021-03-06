from http.client import responses
import os
import json
from datetime import datetime
from warnings import resetwarnings
import google.auth
from dags_conf import dags_conf
from google.auth.transport.requests import AuthorizedSession



AUTH_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

def make_composer2_web_server_request(url, method='GET', **kwargs):
     credentials, _ = google.auth.default(scopes=[AUTH_SCOPE])
     authed_session = AuthorizedSession(credentials)
     # Set the default timeout, if missing
     if 'timeout' not in kwargs:
          kwargs['timeout'] = 90

     return authed_session.request(
          method,
          url,
          **kwargs)

def check_dag_running(webserver_url, dag_id):
     endpoint = f'api/v1/dags/{dag_id}/dagRuns?order_by=-end_date&limit=1'
     request_url = f'{webserver_url}/{endpoint}'
     response = json.loads(make_composer2_web_server_request(request_url, 'GET').text)
     if response.get('dag_runs', None) and len(response['dag_runs']) and response['dag_runs'][0]['state'] == 'running':
          return False
     return True


def trigger_dag(webserver_url, dag_id, data={}, context=None):
     endpoint = f'api/v1/dags/{dag_id}/dagRuns'
     request_url = f'{webserver_url}/{endpoint}'
     json_data = { 'conf': data }
     if check_dag_running(webserver_url, dag_id):
          response = make_composer2_web_server_request(request_url,
               method='POST',
               json=json_data
          )

          if response.status_code == 403:
               raise Exception('You do not have a permission to access this resource.')
          elif response.status_code != 200:
               raise Exception(
                    'Bad request: {!r} / {!r} / {!r}'.format(response.status_code, response.headers, response.text))
          else:
               return response.text
     else:
          print(f"Dag {dag_id} is already running")


def trigger_dag_function(event, context):
    object = str(event['name'])
    object_folder = object.split('/')
    if dags_conf.get(object_folder[0], ''):
        response_text = trigger_dag(
            webserver_url=os.environ.get("WEBSERVER_URL"), dag_id=dags_conf[object_folder[0]])
        print(response_text)
    else:
        print(f"Not found dag for processing that kind of file - {object}.")
