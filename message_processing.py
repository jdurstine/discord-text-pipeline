import re
import string
from collections import defaultdict

import pandas as pd
from nltk import corpus, word_tokenize

import utils

EMOJI_NAME = r":[\w\d]+:"
EMOJI_FULL = r"<a?:[\w\d]+:\d+>"
URL = ""
USER_REF = r"<@:[\d]+>"

def remove_punctuation(message_content):
    punctuation = re.escape(string.punctuation + '’') # adding right handed apostrophe to solve edge case
    msg = re.sub(f'[{punctuation}]?', '', message_content)
    return msg

def extract_emojis(message_content):
    emojis_found = []
    matches = re.findall(EMOJI_FULL, message_content)
    for match in matches:
        name = re.findall(EMOJI_NAME, match)[0]
        right = match.rfind(':')
        emoji_id = match[right+1:-1]
        emojis_found.append((name, emoji_id))
    return emojis_found

def remove_emojis(message_content):
    message = re.sub(EMOJI_FULL, '', message_content)
    return message

def top_words(db_client, limit, user_id, start, end):
    word_dict = defaultdict(int)
    messages = db_client.select_messages(user_id, start, end)

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
    
    return utils.largest_output(df, limit, user_id, 'words')

def top_channels(db_client, limit, user_id, start, end):
    channels = db_client.select_channel_counts(user_id, start, end)
    channels_list = [(row.channel_name, row.count) for row in channels]
    df = pd.DataFrame(channels_list)

    return utils.largest_output(df, limit, user_id, 'channels')