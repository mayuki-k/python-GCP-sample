# TODO:SQLのテーブルとかデータセットの名前消したほうがいいかも？(Jinja2とか使う？)
# TODO:table viewについての調査
# とりあえずコードベースでは残さないようにする

from google.cloud import bigquery
from google.oauth2.service_account import Credentials
from datetime import datetime, timedelta
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
    get_jobs_info()
    # rowsはdictionaryみたいなものだが、keysなどの関数が使えないためpandasに変換するのが妥当か。
    print(rows.to_dataframe().T.to_json())
    get_jobs_info()

def create(table_name):
    table_id = const.get_table_id(table_name)
    schema = [
        bigquery.SchemaField('id', 'INTEGER', mode='REQUIRED'),
        bigquery.SchemaField('name', 'STRING', mode='REQUIRED'),
        bigquery.SchemaField('age', 'INTEGER', mode='REQUIRED')
    ]
    delta = timedelta(days=1)
    # 期限の追加
    expire_day = datetime.now() + delta
    print(expire_day)
    table = bigquery.Table(table_id, schema=schema)
    table.expires = expire_day
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
    print(f'table = {table.expire}')

def get_jobs_info():
    for job in client.list_jobs(max_results=3):
        print('----------')
        print(f'job_id = {job.job_id}')
        print(f'job_type = {job.job_type}')
        print(f'job_state = {job.state}')
        print(f'job table def = {job.table_definitions}')
        print(f'job ref = {job.referenced_tables}')
        print(f'job query = {job.query}')
        # for CREATE/DROP TABLE/VIEW queries.かよ！
        print(f'job target = {job.ddl_target_table}')
        # INSERTはテーブル情報取得できるなあ。SELECTは変なの出る(キャッシュか？)
        print(f'job destination = {job.destination}')
        print('----------')
