import os

from dotenv import load_dotenv

load_dotenv()
ENVIRONMENT = os.getenv('ENVIRONMENT')
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
PROJECT = os.getenv('BIGQUERY_PROJECT')
assert ENVIRONMENT in ('dev', 'prod')