# TODO:SQLのテーブルとかデータセットの名前消したほうがいいかも？(Jinja2とか使う？)
# とりあえずコードベースでは残さないようにする

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
    print(rows.state) # -> RUNNING stateはstr型
    # rowsはdictionaryみたいなものだが、keysなどの関数が使えないためpandasに変換するのが妥当か。
    print(rows.to_dataframe().T.to_json())
    print(rows.state) # -> DONE

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


#insert()
select()
#load('item_data', 'fruits')