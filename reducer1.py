#!/usr/bin/python



###########################################  reducer1.py
### Reduce
# Input: ((docname,term), 1)
# Output: ((term,docname), (N,n))


import sys
#import subprocess
#subprocess.call('dir', shell=True)

current_file = None
current_term = None
term_list = []
current_term_count = 0
current_doc_count = 0
pair = None



for line in sys.stdin:
    
    
    line = line.strip()
    pair, count = line.split('\t', 1)
    
    file, term = pair.split('_', 1)
 
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
 
    if current_file == file:
        current_doc_count += count
        if current_term == term:
            current_term_count += count
        else:
            term_list.append((term, current_term_count))
            current_term = term
            current_term_count = count
    else:
        if current_file:
            # write result to STDOUT
            for term,current_term_count in term_list:
                print '%s\t%s' % (term+'_'+file, str(current_doc_count)+'_'+(str(current_term_count)))
        current_doc_count = count
        current_file = file
        current_term = term
        term_list = []   

if (current_file == file and current_term == term):
    for term,current_term_count in term_list: 
        print '%s\t%s' % (term+'_'+file, str(current_doc_count)+'_'+(str(current_term_count)))

    
    