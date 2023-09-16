import logging
import asyncio
from datetime import timezone, timedelta
from typing import Optional

import discord
from discord import app_commands

import config
from bigquery_connector import bigquery_connector
from utils import message_data, dt_as_utc_str
from message_processing import top_words

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
            latest_dts = self.db_client.latest_message_dts()
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
@app_commands.describe(days = 'Number of days to aggregate over')
async def top(interaction, days: Optional[int]=None):
    """Pull your top 10 most used words."""
    await interaction.response.defer()
    if days is not None:
        start = interaction.created_at - timedelta(days=days)
        end = interaction.created_at
    else:
        start = 'NULL'
        end = 'NULL'
    top_ten = top_words(bigquery, 10, interaction.user.id, start, end)
    await interaction.followup.send(top_ten)

client.run(config.TOKEN)