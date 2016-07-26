#!/bin/bash -e

javac -cp ${HADOOP_HOME}/'*':${HIVE_HOME}/lib/'*' *.java
mkdir -p com/leeriggins/hive/
mv *.class com/leeriggins/hive/
jar cf udf.jar com
rm -rf com
