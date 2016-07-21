create database if not exists ch11 ;

use ch11 ;

set hive.exec.mode.local.auto=true ;

create table if not exists athlete_affiliation(
  name string,
  id string,
  athlete string,
  country string,
  olympics string,
  sport string)
row format delimited
  fields terminated by '\t'
  collection items terminated by ',' ;

load data
local inpath 'data/olympic_athlete_affiliation.tsv'
overwrite into table athlete_affiliation ;

add jar udf.jar ;

create temporary function md5sum as 'com.leeriggins.hive.MD5SumUDF' ;

describe function md5sum ;

describe function extended md5sum ;

select athlete, md5sum(athlete)
from athlete_affiliation
limit 5 ;
