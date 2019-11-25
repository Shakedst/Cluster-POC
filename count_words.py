import re
def count_words(text):
    word_count = {}
    words = re.split(r'\s',text)
    words = filter(lambda x:x!='',words)
    for word in words:
        if word not in word_count.keys():
           word_count[word] = 0
        word_count[word] += 1
    return word_count




