create database if not exists ch14 ;

use ch14 ;

set hive.exec.mode.local.auto = true ;

create table if not exists points(
  x double,
  y double)
row format delimited
  fields terminated by '\t' ;

load data
local inpath 'data/points.tsv'
overwrite into table points ;

add jar udf.jar ;

create temporary function regression as 'com.leeriggins.hive.SimpleRegressionUDAF' ;

describe function regression ;

select regression(x,y) from points ;
