#!/usr/bin/env bash

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/hadoop/tfidf/input \
-output /user/hadoop/tfidf/output1 \
-file /home/hadoop/mapper1.py \
-mapper /home/hadoop/mapper1.py \
-file /home/hadoop/reducer1.py \
-reducer /home/hadoop/reducer1.py

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/hadoop/tfidf/output1 \
-output /user/hadoop/tfidf/output2 \
-file /home/hadoop/mapper2.py \
-mapper /home/hadoop/mapper2.py \
-file /home/hadoop/reducer2.py \
-reducer /home/hadoop/reducer2.py

hadoop jar /usr/lib/hadoop-mapreduce/hadoop-streaming.jar \
-input /user/hadoop/tfidf/output2 \
-output /user/hadoop/tfidf/output \
-file /home/hadoop/mapper3.py \
-mapper /home/hadoop/mapper3.py \
-file /home/hadoop/reducer3.py \
-reducer /home/hadoop/reducer3.py