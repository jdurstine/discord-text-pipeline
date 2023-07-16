import os

import discord
from dotenv import load_dotenv

load_dotenv()
TOKEN = os.getenv('DISCORD_TOKEN')
GUILD = os.getenv('DISCORD_GUILD')

print(TOKEN)
print(GUILD)

intents = discord.Intents.all()
client = discord.Client(intents=intents)

@client.event
async def on_message(message):
    if message.content == '!quit':
        await client.close()
        
    if message.content == '!history':
        async for msg in message.channel.history(limit=10):
            print(msg.content)

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