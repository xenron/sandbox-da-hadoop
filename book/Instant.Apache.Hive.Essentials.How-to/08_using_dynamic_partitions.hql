create database if not exists ch8 ;

use ch8 ;

set hive.exec.mode.local.auto = true ;
set hive.exec.dynamic.partition.mode = nonstrict ;
set hive.exec.max.dynamic.partitions.pernode = 5000 ;

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
overwrite into table athlete_affiliation
;

create table if not exists athlete_by_country(
  name string,
  id string)
partitioned by (country string) ;

insert overwrite table athlete_by_country partition (country)
select name, id, country
from athlete_affiliation ;

show partitions athlete_by_country ;
