#!/usr/bin/python
import sys

for line in sys.stdin:
    pair, vals = line.split('\t')
    term, docname = pair.split('_', 1)
    N, n = vals.split('_')
    try:
        n = int(n)
        N = int(N)
    except ValueError:
        continue
    print('%s\t%s' % (term, docname+'_'+str(N)+'_'+str(n)+'_'+str(1)))
