rm *.txt
hdfs dfs -rm /user/hadoop/tfidf/input/*.txt
python -c 'from text_generator import create_docs; create_docs(number_of_words_per_doc = 100, num_doc = 1000, startnr = 1)'
hdfs dfs -put *.txt /user/hadoop/tfidf/input
pyspark


from nltk.corpus import brown
import random
import math
import time


hardcopy = brown.words()
corpus_length = len(hardcopy)


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


def word_to_key(x):
    file = x[0]
    l = []
    for pair in x[1]:
        l.append((pair[0], (file, pair[1])))
    return l


path = '/user/hadoop/tfidf/input/*.txt'
corpus = sc.wholeTextFiles(path)

time_start = time.time()
clock_start = time.clock()
perf_counter_start = time.perf_counter()
process_time_start = time.process_time()

num_docs = corpus.count()

tf = corpus.map(lambda x: (x[0].split('/')[-1],
                           word_freq(x[1]))).flatMap(word_to_key)


idf = tf.map(lambda x: (x[0], 1)).reduceByKey(
    lambda x, y: x+y).map(lambda x: (x[0], math.log(num_docs/x[1])))


tfidf = tf.join(idf).map(lambda x: (
    x[0], (x[1][0][0], x[1][0][1]*x[1][1]))).collect()


time_end = time.time()
clock_end = time.clock()
perf_counter_end = time.perf_counter()
process_time_end = time.process_time()


print(1000, 100)
print(time_end-time_start)
print(clock_end-clock_start)
print(perf_counter_end-perf_counter_start)
print(process_time_end-process_time_start)
