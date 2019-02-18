#!/usr/bin/python
import os
import sys
# D = ?????
for line in sys.stdin:
    line = line.strip()
    terms = line.split(" ")
    # path = os.environ['mapreduce_map_input_file'].split("/")
    # path = "C:/Users/dorar_000/Documents/
    # GitHub/DB-TF-IDF/data/Aladdin.txt".split('/')
    path = '''/Users/robertaconrad/Documents/
                07_Ecole_Polytechnique_Studienunterlagen/
               Data_Base_Management/DB-TF-IDF/data/Aladdin.txt'''.split(
        '/')
    docname = path[-1]
    for term in terms:
        # term = term.strip('''!()-[]{};:'"\,<>./?@#$%^&*_~''').lower()
        term = term.strip('''!()-[]{};:'",<>./?@#$%^&*_~''').lower()
        print('%s\t%s' % (docname + '_' + term, 1))
