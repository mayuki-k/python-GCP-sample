from google.cloud import datastore
from google.oauth2.service_account import Credentials

from env import const

credentials = Credentials.from_service_account_file(const.PATH)
client = datastore.Client(credentials=credentials, project=credentials.project_id, namespace='stores')

def save(kind, id, datas):
    key = client.key(kind, id)
    put_datas = datastore.Entity(key=key)
    for key in datas.keys():
        put_datas[key] = datas[key]
    client.put(put_datas)

def get_datas(kind, id):
    key = client.key(kind, id)
    datas = client.get(key)
    print(datas)
