create database if not exists ch1;

use ch1;

set hive.exec.mode.local.auto=true ;

create table if not exists city_bid(
  name string,
  id string,
  bidding_city string,
  olympic_games string)
row format delimited fields terminated by '\t'
collection items terminated by ',' ;

load data local inpath 'data/olympic_city_bid.tsv'
overwrite into table city_bid ;

create table bids_per_city (
  bidding_city string, 
  num_bids int) ;

insert overwrite table bids_per_city
select bidding_city, count(1) as num_bids
from city_bid
group by bidding_city
order by num_bids desc, bidding_city asc ;

select * from bids_per_city limit 5 ;
