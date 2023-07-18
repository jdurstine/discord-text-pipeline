import os
import datetime

import discord
from dotenv import load_dotenv

from bigquery_connector import bigquery_connector

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')
PROJECT = os.getenv('BIGQUERY_PROJECT')

intents = discord.Intents.all()
client = discord.Client(intents=intents)

bigquery = bigquery_connector(PROJECT)

def message_data(message):
    etl_dt = datetime.datetime.now()

    if message.reference is not None:
        ref_id = message.reference.message_id
    else:
        ref_id = None
    
    data = {'etl_dt':str(etl_dt),
            'message_id':message.id,
            'created_dt':str(message.created_at),
            'user_id':message.author.id,
            'referenced_message_id':ref_id,
            'content':message.content}
    return data

@client.event
async def on_message(message):

    bigquery.insert_message(message_data(message))

    if message.content == '!quit':
        await client.close()

@client.event
async def on_ready():
    for guild in client.guilds:
        if guild.name == GUILD:
            break
        
    print(
        f'{client.user} is connected to the following guild:\n'
        f'{guild.name}(id: {guild.id})'
    )
    
client.run(TOKEN)

