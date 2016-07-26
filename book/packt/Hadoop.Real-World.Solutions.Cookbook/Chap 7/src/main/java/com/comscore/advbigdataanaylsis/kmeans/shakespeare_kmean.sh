#!/bin/bash
hadoop fs -rmr /user/jowens/shakespeare-seqdir
hadoop fs -rmr /user/jowens/shakespeare-sparse

#convert to sequence files
mahout seqdirectory --input /user/hadoop/shakespeare_text --output /user/hadoop/shakespeare-seqdir --charset utf-8

#make vectors
mahout seq2sparse --input /user/hadoop/shakespeare-seqdir --output /user/hadoop/shakespeare-sparse --namedVector -ml 80 -ng 2 -x 70 -md 1 -s 5 -wt tfidf -a org.apache.lucene.analysis.WhitespaceAnalyzer

#kmeans
mahout kmeans --input /user/hadoop/shakespeare-sparse/tfidf-vectors --output /user/hadoop/shakespeare-kmeans/clusters --clusters /user/hadoop/shakespeare-kmeans/initialclusters --maxIter 10 --numClusters 6 --clustering --overwrite

#dump results
mahout clusterdump --seqFileDir /user/hadoop/shakespeare-kmeans/clusters/clusters-1-final --numWords 5 --dictionary /user/hadoop/shakespeare-sparse/dictionary.file-0 --dictionaryType sequencefile
