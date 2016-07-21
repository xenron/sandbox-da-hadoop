create database if not exists ch6 ;

use ch6 ;

set hive.exec.mode.local.auto=true ;

add jar udf.jar ;

create table if not exists city_bid(
  name string,
  id string,
  bidding_city string,
  olympic_games string
)
row format delimited
fields terminated by '\t'
collection items terminated by ','
;

load data
local inpath 'data/olympic_city_bid.tsv'
overwrite into table city_bid
;

create table city_bid_map(
  name string,
  id string,
  bidding_city string,
  olympic_games string)
row format serde 'com.leeriggins.hive.ColumnarMapSerDe'
location '/data/columnarmap_serde/' ;

insert overwrite table city_bid_map select * from city_bid ;

select * from city_bid limit 5 ;
