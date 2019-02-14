
import sys

current_term = None
doc_list = []
n_list=[]
current_count = 0
term = None
doc = None

for line in sys.stdin:

    term,rest = line.split('\t',1)
    doc,n,count = rest.split('_', 2)
 
    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue
 
    if current_term == term:
        doc_list.append(doc)
        n_list.append(n)
        current_count += count
    else:
        if current_term:
            for i,document in enumerate(doc_list):
                print ('%s\t%s' % (current_term + '_' + document, n_list[i] + '_' + str(current_count)))
            doc_list = []   
            n_list = []
            
        current_count = count
        current_term = term
        doc_list.append(doc)
        n_list.append(n)
        
        

if current_term == term:
    for i,document in enumerate(doc_list):
        print ('%s\t%s' % (current_term + '_' + document, n_list[i] + '_' + str(current_count)))