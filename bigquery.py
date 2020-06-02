from google.cloud import bigquery
from google.oauth2.service_account import Credentials


credentials = Credentials.from_service_account_file('env/auth.json')
client = bigquery.Client(credentials=credentials, project=credentials.project_id)

def insert():
    with open('sql/fruits_insert1.sql') as f:
        query = f.read()
    client.query(query)

def select():
    with open('sql/fruits_select1.sql') as f:
        query = f.read()
    rows = client.query(query)
    # rowsはdictionaryみたいなものだが、keysなどの関数が使えないためpandasに変換するのが妥当か。
    print(rows.to_dataframe().T.to_json())

#insert()
select()