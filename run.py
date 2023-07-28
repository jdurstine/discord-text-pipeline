import discord

import client
import config
from bigquery_connector import bigquery_connector

bigquery = bigquery_connector(config.PROJECT, config.ENVIRONMENT)

intents = discord.Intents.all()
intents.members = True
intents.messages = True
intents.message_content = True

client = client.BeanBotClient(intents=intents, db_client=bigquery, config=config)
client.run(config.TOKEN)