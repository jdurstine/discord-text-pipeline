import logging
import asyncio
import string
from datetime import datetime, timezone
from collections import defaultdict

import discord
import pandas as pd
from nltk import word_tokenize, corpus

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

def extract_urls(message_text):
    pass

def extract_user_refs(message_text):
    pass

def extract_emojis(message_text):
    pass


def top_words(db_client, limit, user_id):
    word_dict = defaultdict(int)
    messages = db_client.select_messages(user_id)

    stopwords = corpus.stopwords.words('english')
    stopwords.extend(list(string.punctuation))
    stopwords.append("'s")
    stopwords.append("'m")
    stopwords.append("n't")
    stopwords.append("'re")
    stopwords.append("https")

    # get count of instances for each token
    for row in messages:
        tokens = word_tokenize(row.msg_content.lower())
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

class BeanBotClient(discord.Client):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = kwargs['config']
        self.db_client = kwargs['db_client']
    
    async def setup_hook(self) -> None:
        # create the background task and run it in the background
        self.bg_task = self.loop.create_task(self.extract_messages())

    async def on_message(self, message):
        if message.content == '!quit':
            await self.close()

        if message.content == '!top':
            top_ten = top_words(self.db_client, 10, message.author.id)
            channel = message.channel
            await channel.send(top_ten)

    async def _pull_messages(self, channel):
        latest_dt = self.db_client.latest_message_dt(channel.id)
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
            for channel in self.guild.channels:
                await self._pull_messages(channel)
            for thread in self.guild.threads:
                await self._pull_messages(thread)
            await asyncio.sleep(60)