rm *.txt
hdfs dfs -rm /user/hadoop/tfidf/input/*.txt
python -c 'from text_generator import create_docs; create_docs(number_of_words_per_doc = 10000, num_doc = 100, startnr = 1)'
hdfs dfs -put *.txt /user/hadoop/tfidf/input
pyspark


import os
import subprocess
import math
import time

time_start = time.time()
clock_start = time.clock()
perf_counter_start = time.perf_counter()
process_time_start = time.process_time()

cmd = 'hdfs dfs -ls /user/hadoop/tfidf/input/'
files = subprocess.check_output(
    cmd, shell=True).decode('utf-8').strip().split('\n')
D = [x.split(' ')[-1] for x in files]
D = D[1:]
#D = glob.glob("/user/hadoop/tfidf/input/*.txt")

num_docs = len(D)
all_words_tf = sc.parallelize([])
all_words_idf = sc.parallelize([])


for doc in D:
    d = sc.textFile(doc)
    words = d.flatMap(lambda s: s.lower().split()).map(lambda w: ''.join(
        x for x in w if x.isalpha()))  # we can do words = doc.split(), d = sc.parralellize(words)
    all_words_idf = all_words_idf.union(words.distinct())
    N = words.count()
    occurrences = words.map(lambda x: (x, 1)).reduceByKey(
        lambda x, y: x+y).map(lambda x: (x[0], (doc, x[1]/N)))
    all_words_tf = all_words_tf.union(occurrences)


idf = all_words_idf.map(lambda x: (x, 1)).reduceByKey(
    lambda x, y: x+y).map(lambda x: (x[0], math.log(num_docs/x[1])))
tfidf = all_words_tf.join(idf).map(lambda x: (
    x[0], (x[1][0][0], x[1][0][1]*x[1][1]))).collect()

time_end = time.time()
clock_end = time.clock()
perf_counter_end = time.perf_counter()
process_time_end = time.process_time()


print(100, 10000)
print(time_end-time_start)
print(clock_end-clock_start)
print(perf_counter_end-perf_counter_start)
print(process_time_end-process_time_start)
