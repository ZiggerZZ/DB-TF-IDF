#!/usr/bin/env python



###########################################  reducer2.py

### Reduce
# Input: (term, (docname,N,n,1))
# Output: ((term,docname), (N,n,d))


import sys

current_term = None
doc_list = []
current_count = 0
term = None
doc = None

for line in sys.stdin:

    term,rest = line.split('\t',1)
    doc,N,n,count = rest.rsplit('_',3)
 
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
 
    if current_term == term:
        doc_list.append((doc,N,n))
        current_count += count
    else:
        if current_term:
            for document,N,n in doc_list:
                print ('%s\t%s' % (current_term + '_' + document, str(N)+'_' + str(n) + '_' + str(current_count))
            doc_list = []   
            n_list = []
            
        current_count = count
        current_term = term
        doc_list.append((doc,N,n))
        
        

if current_term == term:
    for document,N,n in doc_list:
        print ('%s\t%s' % (current_term + '_' + document, str(N)+'_' + str(n) + '_' + str(current_count))