#!/usr/bin/python
from nltk.corpus import brown
import random
corpus_length = len(brown.words())
hardcopy = brown.words()


def create_docs(number_of_words_per_doc=200, num_doc=10, startnr=0):
    # control number of words per doc
    # and number of documents
    # we fix the line length at 20
    line_length = 20
    number_of_lines = int(number_of_words_per_doc / line_length)

    for i in range(0, num_doc):
        # create new file with writing + permission
        new_file = open("textdoc"+str(number_of_words_per_doc) +
                        "words" + str(i+startnr)+".txt", "w+")
        for line in range(0, number_of_lines):
            words = list(map(
                lambda x: hardcopy[x:x+line_length], random.sample(range(corpus_length), line_length)))
            sentences = list(
                map(lambda x: ' '.join(word for word in x), words))
            text = ''.join(map(str, sentences))
            new_file.write(text + "\n")
        new_file.close()
        if (i % 10 == 0):
            print("You created "+str(i)+" files! "+str(num_doc-i)+" left")
