from google.cloud import storage
from google.oauth2.service_account import Credentials

credentials = Credentials.from_service_account_file('env/auth.json')
client = storage.Client(credentials=credentials, project=credentials.project_id)

bucket = client.get_bucket('mayuki-test-bucket')
blob = bucket.blob('data.txt')
blob.upload_from_string('HelloWorld')
