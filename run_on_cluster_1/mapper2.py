import sys

for line in sys.stdin:
    
    term,rest = line.split('_')
    docname,n = rest.split('\t')
    
    try:
        n = int(n)
    except ValueError:
        continue
    
    print('%s\t%s' % (term, docname + '_' + n + '_' + 1))
        
        