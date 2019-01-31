#!/usr/bin/python
import sys
import glob


D = glob.glob("*.txt") #['futurama1.txt', 'futurama2.txt']
num_docs = len(D)
previous = None
sum = 0


current_word = None
current_doc = None
wordcount_in_doc = 0

#stdin has the format ('zooms', 1)
for line in sys.stdin:
    
    mylist = line.split(',')
    print(mylist[0])
    line[0] #contains the word
    line[1] #contains the document it is stored in
    print(type(line))

#initialization
    if current_word is None and current_doc is None:
        current_word = line[0]
        print(current_word)
        current_word = line[1]
    while current_word == line[0] and current_doc == line[1]:
        wordcount_in_doc =+1
#  print("wordcount_in_doc", wordcount_in_doc,"current_word",current_word,"current_doc",current_doc)
    
