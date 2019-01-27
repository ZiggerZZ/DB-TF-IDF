#In this solution we suppose that we don't have many documcents to do "for doc in D". We can have huge documents though
import glob
import math

#there are many methods, for ex strip(). The most powerful is to use regular expressions 
#def clean_word(word):
	#return word.lower().replace('.','').replace(',','').replace('!','').replace('?','').replace('.','').replace('[','').replace(']','').replace('(','').replace(')','').replace('“','').replace('”','').replace('–','').replace('"','').replace(';','').replace("'",'')


#D = glob.glob("*.txt")
D = ['The Princess and the Pea.txt', "The Emperor's New Clothes.txt", 'The Tinderbox.txt']
num_docs = len(D)
all_words_tf = sc.parallelize([])
all_words_idf = sc.parallelize([])
for doc in D:
	d = sc.textFile(doc)
	words = d.flatMap(lambda s: s.lower().split()).map(lambda w: ''.join(x for x in w if x.isalpha())) #we can do words = doc.split(), d = sc.parralellize(words)
	all_words_idf = all_words_idf.union(words.distinct())
	N = words.count()
	occurrences = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0],(doc,x[1]/N)))
	all_words_tf = all_words_tf.union(occurrences)
#all_words_tf.collect()
idf = all_words_idf.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], math.log(num_docs/x[1])))
sorted(all_words_tf.join(idf).map(lambda x: (x[0],(x[1][0][0],x[1][0][1]*x[1][1]))).collect())