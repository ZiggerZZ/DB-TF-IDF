#!/usr/bin/python

import sys
import math

for line in sys.stdin:

    pair, vals = line.split('\t', 1)
    N, n, d = vals.split('_', 2)
    try:
        N = int(N)
        n = int(n)
        d = int(d)
    except ValueError:
        continue
    # to calculate these with normalizing terms we need to compute:
    tf = n/N
    idf = math.log(10000/(1+d))
    tfidf = tf * idf
    print('%s\t%s' % (pair, tfidf))
