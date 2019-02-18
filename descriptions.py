

## Job1: simple wordcount on term-document tuples to compute tf

### Map
# Input: (docname, contents):
# Output: ((docname,term), 1)
###########################################  mapper1.py

###########################################  reducer1.py
### Reduce
# Input: ((docname,term), 1)
# Output: ((term,docname), (N,n))



"""Job2: append document frequency d to term_doc pairs	
### Map	
# Input:  ((term,docname), (N,n))	
# Output: (term, (docname,N,n,1))	
mapper2.py	
"""

"""Job2: append document frequency d to term_doc pairs
### Reduce	### Reduce
# Input:  (term, (docname,N,n,1))	# Input:  (term, (docname,N,n,1))
# Output:  ((term,docname), (N,n,d))	# Output:  ((term,docname), (N,n,d))
"""

## Job3: compute tf-idfs

### Map
# Input:  ((term,docname), (N,n,d))
# Output: ((term,docname), tfidf)

###########################################  mapper3.py


###########################################  reducer3.py

### Reduce
# Input:((term,docname), tfidf)
# Output: ((term,docname), tfidf)

