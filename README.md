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