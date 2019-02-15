#!/usr/bin/env python


## Job1: simple wordcount on term-document tuples to compute tf

### Map
# Input: (docname, contents):
# Output: ((docname,term), 1)


###########################################  mapper1.py

import os
import sys
import glob
import subprocess
#subprocess.call('dir', shell=True)

### D = ?????

for line in sys.stdin:
    
    line = line.strip()
    terms = line.split(" ")
    
    #path = os.environ['mapreduce_map_input_file'].split('/')
    path = "C:/Users/dorar_000/Documents/GitHub/DB-TF-IDF/data/Aladdin.txt".split('/')
    
    docname = path[-1]
    
    for term in terms:
        term = term.strip('''!()-[]{};:'"\,<>./?@#$%^&*_~''').lower()
        print('%s\t%s' % (term + '_' + docname, 1))
