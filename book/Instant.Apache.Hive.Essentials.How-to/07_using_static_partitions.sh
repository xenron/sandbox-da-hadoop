#!/bin/bash
#
# Prepares the split input data for statically partitioned tables and copies the files into HDFS.

split -a 1 -l 25000 data/olympic_athlete.tsv data/olympic_athlete_split_
split -a 1 -l 7000 data/olympic_athlete_affiliation.tsv data/olympic_athlete_affiliation_split_

for p in a b c ; do
  $HADOOP_HOME/bin/hadoop fs -mkdir /data/athlete/part_key=$p/
  $HADOOP_HOME/bin/hadoop fs -put data/olympic_athlete_split_$p /data/athlete/part_key=$p/
done

for p in a b c ; do
  $HADOOP_HOME/bin/hadoop fs -mkdir /data/athlete_affiliation/part_key=$p/
  $HADOOP_HOME/bin/hadoop fs -put data/olympic_athlete_affiliation_split_$p /data/athlete_affiliation/part_key=$p/
done

