import os
import sys

for line in sys.stdin:
    
    line = line.strip()
    terms = line.split(" ")
       
    path = os.environ['mapreduce_map_input_file'].split('/')
    docname = path[-1]
    
    for term in terms:
        term = term.strip('''!()-[]{};:'"\,<>./?@#$%^&*_~''').lower()
        print('%s\t%s' % (term + '_' + docname, 1))
        