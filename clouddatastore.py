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

# 名前entityとかの方がいいか？
def save_multi(kind, ids, datas_list):
    keys = []
    put_datas_list = []
    for id in ids:
        keys.append(client.key(kind, id))
    for i in range(len(keys)):
        datas = datas_list[i]
        put_datas = datastore.Entity(key=keys[i])
        data_keys = datas.keys()
        for data_key in data_keys:
            put_datas[data_key] = datas[data_key]
        put_datas_list.append(put_datas)
    client.put_multi(put_datas_list)
    

def get_datas(kind, id):
    key = client.key(kind, id)
    datas = client.get(key)
    print(datas)

def get_multi(kind, ids):
    keys = []
    for id in ids:
        keys.append(client.key(kind, id))
    datas = client.get_multi(keys)
    print(datas)

def delete(kind, id):
    key = client.key(kind, id)
    client.delete(key)

def delete_multi(kind, ids):
    keys = []
    for id in ids:
        keys.append(client.key(kind, id))
    datas = client.delete_multi(keys)