create database if not exists ch5 ;

use ch5 ;

set hive.exec.mode.local.auto = true ;

create table if not exists city_bid(
  name string,
  id string,
  bidding_city string,
  olympic_games string)
row format serde 'org.apache.hadoop.hive.serde2.RegexSerDe'
  with serdeproperties(
  'input.regex' = '([^t]*)\t([^\t]*)\t([^\t]*)\t([^\t]*)')
stored as textfile ;

load data
local inpath 'data/olympic_city_bid.tsv'
overwrite into table city_bid ;

create table if not exists city_bid_seqfile(
  name string,
  id string,
  bidding_city string,
  olympic_games string)
row format delimited
  fields terminated by '\t'
  collection items terminated by '\002'
  map keys terminated by '\003'
stored as sequencefile ;

create table if not exists city_bid_rcfile(
  name string,
  id string,
  bidding_city string,
  olympic_games string)
stored as rcfile ;

from city_bid
insert overwrite table city_bid_seqfile select *
insert overwrite table city_bid_rcfile select * ;

select * from city_bid_seqfile limit 2 ;

select * from city_bid_rcfile limit 2 ;
