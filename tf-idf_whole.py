import math

def word_freq(doc):
    lines = doc.lower().split('\n')
    words = []
    for line in lines:
        for word in line.split():
            clean_word = ''.join(x for x in word if x.isalpha())
            words.append(clean_word)
    occurrences = []
    for word in words:
        occurrences.append(words.count(word))
    N = len(words)
    freqs = [w/N for w in occurrences]
    wordfreq = set(zip(words, freqs))
    return wordfreq


#words = d.flatMap(lambda s: s.lower().split()).map(
#    lambda w: ''.join(x for x in w if x.isalpha()))


def word_to_key(x):
    file = x[0]
    l = []
    for pair in x[1]:
        l.append((pair[0], (file, pair[1])))
    return l


corpus = sc.wholeTextFiles('/user/hadoop/tfidf/texts/*')
num_docs = corpus.count()

tf = corpus.map(lambda x: (x[0].split('/')[-1],
                           word_freq(x[1]))).flatMap(word_to_key)
idf = tf.map(lambda x: (x[0], 1)).reduceByKey(
    lambda x, y: x+y).map(lambda x: (x[0], math.log(num_docs/x[1])))
sorted(tf.join(idf).map(lambda x: (
    x[0], (x[1][0][0], x[1][0][1]*x[1][1]))).collect())
