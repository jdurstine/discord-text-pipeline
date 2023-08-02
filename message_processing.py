import re
import string

EMOJI_NAME = r":[\w\d]+:"
EMOJI_FULL = r"<a?:[\w\d]+:\d+>"
URL = ""
USER_REF = r"<@:[\d]+>"

def remove_punctuation(message_content):
    punctuation = re.escape(string.punctuation)
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

x = '.'
print(x)
print(remove_punctuation(x))