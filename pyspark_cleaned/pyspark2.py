# if small docs then create one big RDD with all docs (for ex prepro)
# preprocessing use map-reduce, tf-idf do spark. map-reduce put in /output, after spark use /output
# preproc put name of doc on each line,  'name of doc' \t 'line'

# In this solution we suppose that we don't have many documcents to do "for doc in D". We can have huge documents though

# there are many methods, for ex strip(). The most powerful is to use regular expressions
# def clean_word(word):
# return word.lower().replace('.','').replace(',','').replace('!','').replace('?','').replace('.','').replace('[','').replace(']','').replace('(','').replace(')','').replace('“','').replace('”','').replace('–','').replace('"','').replace(';','').replace("'",'')

#	D = glob.glob("*.txt")
#D = ['The Princess and the Pea.txt', "The Emperor's New Clothes.txt", 'The Tinderbox.txt']

# return list = [(word,freq),(word,freq),...]

#from string import punctuation

def word_freq(doc):
    lines = doc.lower().split('\n')
    words = []
    for line in lines:
        for word in line.split():
            #characters_to_remove = list(punctuation)
            #transformation_dict = {initial:'' for initial in characters_to_remove}
            #no_punctuation_word = word.translate(str.maketrans(transformation_dict))
            #words.append(no_punctuation_word)
            clean_word = ''.join(x for x in word if x.isalpha())
            words.append(clean_word)
    occurrences = []
    for word in words:
        occurrences.append(words.count(word))
    N = len(words)
    freqs = [w/N for w in occurrences]
    wordfreq = set(zip(words, freqs))
    return wordfreq

words = d.flatMap(lambda s: s.lower().split()).map(lambda w: ''.join(x for x in w if x.isalpha()))

def word_to_key(x):
    file = x[0]
    l = []
    for pair in x[1]:
        l.append((pair[0], (file, pair[1])))
    return l


corpus = sc.wholeTextFiles('texts/*.txt')
num_docs = corpus.count()

tf = corpus.map(lambda x: (x[0].split('/')[-1], word_freq(x[1]))).flatMap(word_to_key)
idf = tf.map(lambda x: (x[0], 1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[0], math.log(num_docs/x[1])))
sorted(tf.join(idf).map(lambda x: (x[0], (x[1][0][0], x[1][0][1]*x[1][1]))).collect())
