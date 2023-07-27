import os
import re
from datetime import datetime, timezone
from collections import defaultdict

import discord
from discord.ext import commands

import pandas as pd
from dotenv import load_dotenv
from nltk import word_tokenize

import config
from bigquery_connector import bigquery_connector

intents = discord.Intents.all()
intents.members = True
intents.messages = True
intents.message_content = True
client = discord.Client(intents=intents)

bigquery = bigquery_connector(config.PROJECT, config.ENVIRONMENT)

def dt_as_utc_str(datetime_obj):
    format = '%Y-%m-%d %H:%M:%S.%f'
    utc_dt = datetime_obj.astimezone(timezone.utc)
    return utc_dt.strftime(format)

def message_data(message):
    etl_dt = datetime.now()

    if message.reference is not None:
        ref_id = message.reference.message_id
    else:
        ref_id = None
    
    data = {'etl_dt':dt_as_utc_str(etl_dt),
            'msg_id':message.id,
            'msg_created_dt':dt_as_utc_str(message.created_at),
            'channel_id':message.channel.id,
            'channel_name':message.channel.name,
            'user_id':message.author.id,
            'user_global_name':message.author.global_name,
            'user_display_name':message.author.display_name,
            'user_nickname':message.author.nick,
            'user_name':message.author.name,
            'ref_msg_id':ref_id,
            'msg_content':message.content}
    return data

async def msg_count(channel):
    count = 0
    try:
        async for message in channel.history(limit=None):
            count += 1
        return count
    except AttributeError:
        return None

def top_words(limit, user_id):
    word_dict = defaultdict(int)
    messages = bigquery.select_messages(user_id)
    
    for row in messages:
        tokens = word_tokenize(row.msg_content)
        for word in tokens:
            word_dict[word] += 1
    
    word_list = [(word, word_dict[word]) for word in word_dict]
    df = pd.DataFrame(word_list)
    largest = df.nlargest(limit, 1)
    
    output = f"### <@{user_id}>'s Top 10 Words\n"
    for i in range(len(largest)):
        word = largest.iloc[i, 0]
        count = largest.iloc[i, 1]
        output = output + f'{i}. {word} - {count}\n'
    return output

"""
def row_to_string(df, row, columns):
    for col in columns:

    row_string = row_string


def df_to_list(df, columns = None):
    output = ""
    if columns is None:
        columns = [i for i in range(len(df.columns))]
    for i in range(len(df)):
        row_string = ""
        row_string = row_string +
        output = f'{i}. '
"""

@client.event
async def on_message(message):

    bigquery.insert_message(message_data(message))

    if message.content == '!quit':
        await client.close()

    if message.content == '!top':
        top_ten = top_words(10, message.author.id)
        channel = message.channel
        await channel.send(top_ten)

@client.event
async def on_ready():
    for guild in client.guilds:
        if guild.name == config.GUILD:
            break
        
    print(
        f'{client.user} is connected to the following guild:\n'
        f'{guild.name}(id: {guild.id})'
    )
    """
    channel = discord.utils.find(lambda c: c.name == 'generally-cats', guild.channels)
    count = await msg_count(channel)
    print(f'{channel.name}: {count}')
    """
    """for channel in guild.channels:
        count = await msg_count(channel)
        print(f'{channel.name}: {count}')"""

client.run(config.TOKEN)

