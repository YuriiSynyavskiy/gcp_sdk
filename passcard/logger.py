from dotenv import dotenv_values
from google.cloud.logging_v2 import Client as LoggingClient
from google.oauth2 import service_account

config = dotenv_values('.env')

credentials = service_account.Credentials.from_service_account_file(config.get('GOOGLE_APPLICATION_CREDENTIALS'))
client = LoggingClient(credentials=credentials)

logger = client.logger('passcard')
