from google.cloud import bigquery
from google.oauth2.service_account import Credentials


credentials = Credentials.from_service_account_file('env/auth.json')
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def insert():
    with open('sql/fruits_insert1.sql') as f:
        query = f.read()
    client.query(query)

insert()