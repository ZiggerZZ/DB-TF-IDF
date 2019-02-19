#!/usr/bin/python
from nltk.corpus import brown
import random
corpus_length = len(brown.words())

# control number of words per doc
number_of_words_per_doc = 200
# and number of documents
num_doc = 10

#we fix the line length at 20
line_length = 20
number_of_lines = int(number_of_words_per_doc / line_length)

for i in range(0, num_doc):
    #create new file with writing + permission
    new_file = open("textdoc"+str(i)+".txt", "w+")
    for line in range(0, number_of_lines):
        index = random.randint(1, corpus_length)
        words = brown.words()[index: index+20]
        sentence = ' '.join(word for word in words)
        new_file.write(sentence + "\n")

    new_file.close()
