create database if not exists ch12 ;

use ch12 ;

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

create temporary function generic_md5sum as 'com.leeriggins.hive.GenericMD5SumUDF' ;

describe function generic_md5sum ;

describe function extended generic_md5sum ;

select athlete, country, generic_md5sum(athlete), generic_md5sum(athlete, country)
from athlete_affiliation
limit 5 ;
