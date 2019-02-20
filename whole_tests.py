#run directly in spark

from nltk.corpus import brown
import random
import math
import time

#from pyspark import SparkConf
#from pyspark.context import SparkContext
#sc = SparkContext.getOrCreate(SparkConf())


hardcopy = brown.words()
corpus_length = len(hardcopy)


def create_docs(number_of_words_per_doc=200, num_doc=10, startnr=0):
    # control number of words per doc
    # and number of documents
    # we fix the line length at 20
    line_length = 20
    number_of_lines = int(number_of_words_per_doc / line_length)

    for i in range(0, num_doc):
        # create new file with writing + permission
        new_file = open("textdoc"+str(number_of_words_per_doc) +
                        "words" + str(i+startnr)+".txt", "w+")
        for line in range(0, number_of_lines):
            words = list(map(
                lambda x: hardcopy[x:x+line_length], random.sample(range(corpus_length), line_length)))
            sentences = list(
                map(lambda x: ' '.join(word for word in x), words))
            text = ''.join(map(str, sentences))
            new_file.write(text + "\n")
        new_file.close()
        if (i % 10 == 0):
            print("You created "+str(i)+" files! "+str(num_doc-i)+" left")


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


number_of_docs = [5,100,200,300]
NumWordsPerDoc = [100,200,300]
results = {}

#we can optimize document generation but who cares
for nod in number_of_docs:
    for nwpd in NumWordsPerDoc:
        create_docs(number_of_words_per_doc=nwpd, num_doc=nod, startnr=0):
        corpus = sc.wholeTextFiles('/user/hadoop/tfidf/input/*.txt')

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
        results[(number_of_docs,NumWordsPerDoc)] = [time_end-time_start, clock_start-clock_end, perf_counter_start-perf_counter_end, process_time_start-process_time_end]

print(results)