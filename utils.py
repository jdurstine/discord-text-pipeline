from datetime import timezone, datetime

import discord

def dt_as_utc_str(datetime_obj):
    format = '%Y-%m-%d %H:%M:%S.%f'
    utc_dt = datetime_obj.astimezone(timezone.utc)
    return utc_dt.strftime(format)

def channel_data(channel, etl_dt):

    data = {
        'etl_dt':dt_as_utc_str(etl_dt),
        'guild_id':channel.guild.id,
        'channel_id':channel.id,
        'channel_name':channel.name,
        # waiting for wrapper to support voice channel status
        'channel_status':None, 
        'channel_type':f'{channel.type}'
    }

    return data

def voice_data(channel, etl_dt, member = None):

    if member is not None:
        user_id = member.id
    else:
        user_id = None

    data = {'etl_dt':dt_as_utc_str(etl_dt),
            'channel_id':channel.id,
            'channel_name':channel.name,
            'user_id':user_id}
    
    return data

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