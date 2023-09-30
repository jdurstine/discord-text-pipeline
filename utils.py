from datetime import timezone, datetime

import discord

def dt_as_utc_str(datetime_obj):
    format = '%Y-%m-%d %H:%M:%S.%f'
    utc_dt = datetime_obj.astimezone(timezone.utc)
    return utc_dt.strftime(format)

def largest_output(df, limit, user_id, entity_name):
    """Takes in a 2 column dataframe of (entity, count), orders it,
    then returns a ranking formatted for discord."""
    
    if len(df) > 0:
        # truncate data to words with largest counts
        # will not work if there is no data
        largest = df.nlargest(limit, 1)

        # format output for discord
        output = f"### <@{user_id}>'s Top 10 {entity_name.capitalize()}\n"
        for i in range(len(largest)):
            entity = largest.iloc[i, 0]
            count = largest.iloc[i, 1]
            output = output + f'{i}. {entity} - {count}\n'
    else:
        # inform the user no words were found
        output = f'No {entity_name.lower()} found in given timeframe for <@{user_id}>'
    return output

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

def member_data(member, etl_dt):

    data = {'etl_dt':dt_as_utc_str(etl_dt),
            'guild_id':member.guild.id,
            'member_id':member.id,
            'member_global_name':member.global_name,
            'member_display_name':member.display_name,
            'member_nickname':member.nick,
            'member_name':member.name}

    return data

def message_data(message, etl_dt):
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