#!/bin/sh
#Script From : http://wiki.apache.org/hadoop/topology_rack_awareness_scripts
#Credit : Apache Wiki

HADOOP_CONF=/home/shashwat/hadoop

while [ $# -gt 0 ] ; do
  nodeArg=$1
  exec< ${HADOOP_CONF}/IPRackFile.list 
  result="" 
  while read line ; do
    ar=( $line ) 
    if [ "${ar[0]}" = "$nodeArg" ] ; then
      result="${ar[1]}"
    fi
  done 
  shift 
  if [ -z "$result" ] ; then
    echo -n "/default/rack "
  else
    echo -n "$result "
  fi
done 