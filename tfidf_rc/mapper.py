#!/usr/bin/python
import sys
import re
import os
import glob


D = glob.glob("*.txt") #['futurama1.txt', 'futurama2.txt']
num_docs = len(D)

#read in each document in the file repo
file_nr = 0
for doc in D:
    file = open(doc)
    for line in file:
        line = re.sub(r'^\W+|\W+$', '',line)
        words = re.split(r"\W+",line)
    
    #check which document the word comes from
    #each word is a key, with the corresponding index of the document
    
        for word in words:
            if len(word)>0:
                print (word.lower(),file_nr)
    file_nr = file_nr+1
