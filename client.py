import logging
import asyncio
import string
from datetime import datetime, timezone
from collections import defaultdict

import discord
from discord import app_commands
import pandas as pd
from bigquery_connector import bigquery_connector
from nltk import word_tokenize, corpus

import config
from message_processing import remove_emojis, remove_punctuation

def dt_as_utc_str(datetime_obj):
    format = '%Y-%m-%d %H:%M:%S.%f'
    utc_dt = datetime_obj.astimezone(timezone.utc)
    return utc_dt.strftime(format)

def message_data(message):
    etl_dt = datetime.utcnow()

    if message.reference is not None:
        ref_id = message.reference.message_id
    else:
        ref_id = None

    # author can be global user or member so we're setting
    # a nickname variable to avoid an attribute error from discord.User
    if type(message.author) is not discord.Member:
        nickname = message.author.display_name
    else:
        nickname = message.author.nick

    data = {'etl_dt':dt_as_utc_str(etl_dt),
            'msg_id':message.id,
            'msg_created_dt':dt_as_utc_str(message.created_at),
            'channel_id':message.channel.id,
            'channel_name':message.channel.name,
            'user_id':message.author.id,
            'user_global_name':message.author.global_name,
            'user_display_name':message.author.display_name,
            'user_nickname':nickname,
            'user_name':message.author.name,
            'ref_msg_id':ref_id,
            'msg_content':message.content}
    return data

def top_words(db_client, limit, user_id):
    word_dict = defaultdict(int)
    messages = db_client.select_messages(user_id)

    stopwords = corpus.stopwords.words('english')

    # get count of instances for each token
    for row in messages:
        msg = row.msg_content.lower()
        msg = remove_emojis(msg)
        msg = remove_punctuation(msg)
        tokens = word_tokenize(msg)
        tokens = [word for word in tokens if word not in stopwords]
        for word in tokens:
            word_dict[word] += 1
    
    # convert dictionary to list of tupes for use in dataframe and find top n counts
    word_list = [(word, word_dict[word]) for word in word_dict]
    df = pd.DataFrame(word_list)
    largest = df.nlargest(limit, 1)
    
    # format output for discord
    output = f"### <@{user_id}>'s Top 10 Words\n"
    for i in range(len(largest)):
        word = largest.iloc[i, 0]
        count = largest.iloc[i, 1]
        output = output + f'{i}. {word} - {count}\n'
    return output

bigquery = bigquery_connector(config.PROJECT, config.ENVIRONMENT)

intents = discord.Intents.all()
intents.members = True
intents.messages = True
intents.message_content = True

class BeanBotClient(discord.Client):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = kwargs['config']
        self.db_client = kwargs['db_client']
        self.tree = app_commands.CommandTree(self)

    async def setup_hook(self) -> None:
        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.extract_messages())

        # set up the command tree and sync all our commands to the chosen guild
        guild = discord.Object(id=self.config.GUILD_ID)
        self.tree.copy_global_to(guild=guild)
        await self.tree.sync(guild=guild)

    async def _pull_messages(self, channel, latest_dt):
        if latest_dt is not None:
            latest_dt = latest_dt.replace(tzinfo=timezone.utc)
        try:
            count = 0
            async for message in channel.history(limit=None, after=latest_dt, oldest_first=True):
                self.db_client.insert_message(message_data(message))
                count += 1
        except Exception as inst:
            logging.warning(f"Failed loading messages from {self.guild.name} - {channel.name} - {inst}")
        else:
            logging.info(f"Loaded {count} messages from {self.guild.name} - {channel.name}")

    async def extract_messages(self):
        await self.wait_until_ready()
        
        # placing here to ensure it gets set before calling guild.channels/guild.threads
        # can this be placed somewhere better?
        for guild in self.guilds:
            if guild.name == self.config.GUILD:
                self.guild = guild 
        
        while not self.is_closed():
            latest_dts = self.client_db.latest_message_dts()
            for channel in self.guild.channels:
                try:
                    latest_dt = latest_dts[channel.id]
                except KeyError:
                    latest_dt = None
                await self._pull_messages(channel, latest_dt)
            for thread in self.guild.threads:
                try:
                    latest_dt = latest_dts[channel.id]
                except KeyError:
                    latest_dt = None
                await self._pull_messages(thread, latest_dt)
            await asyncio.sleep(15 * 60)

bigquery = bigquery_connector(config.PROJECT, config.ENVIRONMENT)

intents = discord.Intents.all()
intents.members = True
intents.messages = True
intents.message_content = True

discord.utils.setup_logging()
client = BeanBotClient(intents=intents, db_client=bigquery, config=config)

@client.tree.command()
async def top(interaction):
    top_ten = top_words(bigquery, 10, interaction.user.id)
    await interaction.response.send_message(top_ten)

client.run(config.TOKEN)