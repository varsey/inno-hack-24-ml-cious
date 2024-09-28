import re
import string
import nltk


nltk.download('stopwords')
stop_words = set(nltk.corpus.stopwords.words('russian'))

LEN_THRESHOLD = 3

flt_chars = []

def remove_punctuation(input_string):
    combined_punctuation = string.punctuation + ''.join(flt_chars)
    translator = str.maketrans(combined_punctuation, ' ' * len(combined_punctuation))
    return input_string.translate(translator)

def simple_process_item(x: str, exclude: list):
    if x is None:
        return  ''
    x = x.lower()
    x = remove_punctuation(x)
    item = ' '.join(re.split(r'(\d+)', x))
    while item.count(2 * " ") > 0:
        item = item.replace(2 * " ", " ")
    item = item.replace(' .', '.').replace('. ', '.')
    return ' '.join([x for x in item.split() if x not in exclude and len(x) > LEN_THRESHOLD and not x[0].isdigit()])


def clean_phone_number(phone):
    # Use regex to find all digits and join them into a single string
    return ''.join(re.findall(r'\d+', phone))

def clean_bithdate(phone):
    # Use regex to find all digits and join them into a single string
    return ' '.join(re.findall(r'\d+', phone))


