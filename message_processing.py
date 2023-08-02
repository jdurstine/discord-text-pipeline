import re
from enum import Enum

class RE(Enum):
    EMOJI_NAME = ":[\w\d]+:"
    EMOJI_FULL = "<:[\w\d]+:[\d]+>"
    URL = ""
    USER_REF = "<@:[\d]+>"

def extract_emoji(message_contents):
    