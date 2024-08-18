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

def count_words(messages):

    word_dict = defaultdict(int)
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

    return word_dict

def top_words(db_client, limit, user_id, start, end):
    
    messages = db_client.select_messages(user_id, start, end)
    word_counts = count_words(messages)
    
    # convert dictionary to list of tuples for use in dataframe and find top n counts
    word_list = [(word, word_counts[word]) for word in word_counts]
    df = pd.DataFrame(word_list)
    
    return utils.largest_output(df, limit, user_id, 'words')

def top_channels(db_client, limit, user_id, start, end):
    channels = db_client.select_channel_counts(user_id, start, end)
    channels_list = [(row.channel_name, row.count) for row in channels]
    df = pd.DataFrame(channels_list)

    return utils.largest_output(df, limit, user_id, 'channels')

def average_term_use_per_message(word_counts, total_messages):
    pass

def top_unique_words(db_client, limit, user_id):

    # 1. calculate corpus average uses per message E(T_c)
    # 2. calculate individual corpus average uses per message E(T_p)
    # 3. rank words by dE = E(T_p) - E(T_c)
    
    # corpus_total_messages = db_client.total_message_count()
    corpus_word_counts = count_words(db_client.select_messages(userid=None))
    corpus_total_word_count = sum(corpus_word_counts.values())
    corpus_word_percentages = {word: corpus_word_counts[word]/float(corpus_total_word_count)
                               for word in corpus_word_counts}
    
    # user_total_messages = db_client.message_count(user_id)
    user_word_counts = count_words(db_client.select_messages(userid=user_id))
    user_total_word_count = sum(user_word_counts.values())
    user_word_percentages = {word: user_word_counts[word]/float(user_total_word_count) for word in user_word_counts}

    user_adjusted_word_percentages = {word: user_word_percentages[word] - corpus_word_percentages[word]
                                      for word in user_word_percentages}
    
    user_adjusted_word_percentages = pd.DataFrame([(word, user_adjusted_word_percentages[word])
                                                     for word in user_adjusted_word_percentages])

    return utils.largest_output(user_adjusted_word_percentages, limit, user_id, 'unique words')