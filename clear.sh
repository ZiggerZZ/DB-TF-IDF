#!/usr/bin/env bash

hdfs dfs -rm -r /user/hadoop/tfidf/input
hdfs dfs -rm -r /user/hadoop/tfidf/output
hdfs dfs -rm -r /user/hadoop/tfidf/output1
hdfs dfs -rm -r /user/hadoop/tfidf/output2
hdfs dfs -mkdir /user/hadoop/tfidf/input