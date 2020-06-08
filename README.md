# python-GCP-sample
pythonのGCP関連のsample

# 環境構築

```
pip install google-auth, google-cloud
```

# bigquery.py

BigQueryでデータセット、テーブルを作成

```sql
CREATE TABLE item_data.fruits(
  id int64,
  name string,
  price int64
)
```

# clouddatastore.py

Cloud DataStore

- 最初作るときにFireStoreモードとDataStoreモードの2つがあり、DataStoreモードを選択
- エンティティを作成する
- 名前空間、種類を指定
- プロパティを適当に指定