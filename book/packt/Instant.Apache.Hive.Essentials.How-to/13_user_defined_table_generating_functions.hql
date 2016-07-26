create database if not exists ch13 ;

use ch13 ;

set hive.exec.mode.local.auto = true ;

create table if not exists athlete_affiliation(
  name string,
  id string,
  athlete string,
  country string,
  olympics string,
  sport string)
row format delimited
  fields terminated by '\t' ;

load data
local inpath 'data/olympic_athlete_affiliation.tsv'
overwrite into table athlete_affiliation ;

add jar udf.jar ;

create temporary function char_bigrams as 'com.leeriggins.hive.CharBigramsUDTF' ;

describe function char_bigrams ;

create table if not exists bigram_freq_by_country(
  country string,
  bigram string,
  freq int);

insert overwrite table bigram_freq_by_country
select
  country,
  bigram,
  count(1) as freq
from athlete_affiliation
  lateral view char_bigrams(lower(athlete)) t as bigram
group by country, bigram ;

select bigram, freq
from bigram_freq_by_country 
where country = "United States of America" 
order by freq desc 
limit 5 ;

select bigram, freq
from bigram_freq_by_country 
where country = "China" 
order by freq desc 
limit 5 ;

select bigram, freq
from bigram_freq_by_country 
where country = "Russia"
order by freq desc 
limit 5 ;

