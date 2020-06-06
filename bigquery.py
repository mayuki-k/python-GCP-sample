# TODO:SQLのテーブルとかデータセットの名前消したほうがいいかも？(Jinja2とか使う？)
# TODO:table viewについての調査
# とりあえずコードベースでは残さないようにする

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
# 定数ファイル。作成が必要
from env import const

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
    print(rows.state) # -> RUNNING stateはstr型
    # rowsはdictionaryみたいなものだが、keysなどの関数が使えないためpandasに変換するのが妥当か。
    print(rows.to_dataframe().T.to_json())
    print(rows.state) # -> DONE

def create(table_name):
    table_id = const.get_table_id(table_name)
    schema = [
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED')
    ]
    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)


def load(dataset:str, table:str):
    """localのファイルからデータをロード

    Arguments:
        dataset {str} -- データセット名
        table {str} -- テーブル名
    """    
    filename = 'data.csv'
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    with open(filename, "rb") as f:
        job = client.load_table_from_file(f, table_ref, job_config=job_config)

def get_table_info(table_name):
    table_id = const.get_table_id(table_name)
    table = client.get_table(table_id)
    print(f'schema = {table.schema}')
    print(f'project_id = {table.project}')
    print(f'dataset_name = {table.dataset_id}')
    print(f'table = {table.table_id}')

get_table_info('fruits')