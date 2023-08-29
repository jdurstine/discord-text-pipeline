from collections import defaultdict

import config
from bigquery_connector import bigquery_connector

import re
import string
from collections import defaultdict

import pandas as pd
import numpy as np
from nltk import word_tokenize, corpus

EMOJI_NAME = r":[\w\d]+:"
EMOJI_FULL = r"<a?:[\w\d]+:\d+>"
URL = ""
USER_REF = r"<@:[\d]+>"

def remove_punctuation(message_content):
    punctuation = re.escape(string.punctuation + '’')
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

def get_word_counts(message):
    word_dict = defaultdict(int)
    stopwords = corpus.stopwords.words('english')

    # get count of instances for each token
    msg = message.lower()
    msg = remove_emojis(msg)
    msg = remove_punctuation(msg)
    tokens = word_tokenize(msg)
    tokens = [word for word in tokens if word not in stopwords]
    for word in tokens:
        word_dict[word] += 1

    return word_dict

def get_word_counts_bulk(messages):
    word_dict = defaultdict(int)
    stopwords = corpus.stopwords.words('english')

    # get count of instances for each token
    for msg in messages:
        msg = msg.msg_content.lower()
        msg = remove_emojis(msg)
        msg = remove_punctuation(msg)
        tokens = word_tokenize(msg)
        tokens = [word for word in tokens if word not in stopwords]
        for word in tokens:
            word_dict[word] += 1

    return word_dict

def top_words(db_client, limit, user_id):
    
    messages = db_client.select_messages(user_id)
    word_dict = get_word_counts(messages)
    
    # convert dictionary to list of tuples for use in dataframe and find top n counts
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

"""
1. Calculate word frequencies for all users
2. Calculate document frequency for each word
3. Calculate inverse document frequency for each word
"""

bigquery = bigquery_connector(config.PROJECT, 'prod')

user_ids = bigquery.unique_users()



test_dict = {
    'a': {'one':1, 'two':2},
    'b': {'two':2, 'three':3},
    'c': {'two':2}
}

# Get word counts per message
ids = [row.user_id for row in user_ids]

total_messages = 0
total_word_count = defaultdict(int)

for id in ids:
    messages = bigquery.select_messages(id)
    for msg in messages:
        total_messages += 1
        msg_word_count = get_word_counts(msg.msg_content)
        for word in msg_word_count:
            total_word_count[word] += 1

# Calculate idf per word
inv_doc_freq = defaultdict(int)
for word in total_word_count:
    inv_doc_freq[word] = np.log(total_messages/(total_word_count[word] + 1)) + 1
inv_doc_freq_df = pd.Series(inv_doc_freq, name='idf')

# Use idf to rank top words for several users
# Zahren - 161566565110775809
# Crow - 303542480018604051
# Bumble - 313417234057920523

ids = [161566565110775809, 303542480018604051, 313417234057920523]
for id in ids:
    z_msgs = bigquery.select_messages(id)
    z_counts = get_word_counts_bulk(z_msgs)
    z_df = pd.Series(z_counts, name='user_word_counts')
    z_df = pd.concat([z_df, inv_doc_freq_df], axis=1)
    z_df['tf_idf'] = z_df['user_word_counts'] * z_df['idf']
    z_top = z_df.nlargest(n=10, columns='tf_idf')
    z_top_old = z_df.nlargest(n=10, columns='user_word_counts')
    print(z_top)
    print(z_top_old)
