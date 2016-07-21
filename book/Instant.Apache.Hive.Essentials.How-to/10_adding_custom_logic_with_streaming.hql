create database if not exists ch10 ;

use ch10 ;

set hive.exec.mode.local.auto = true ;

create table if not exists athlete(
  name string,
  id string,
  demonstration_events_competed_in array<string>,
  demonstration_medals_won array<string>,
  country array<string>,
  medals_won array<string>)
row format delimited
  fields terminated by '\t'
  collection items terminated by ',' ;

load data
local inpath 'data/olympic_athlete.tsv'
overwrite into table athlete ;

add file token_lengths.rb ;

add file average_unique_lengths.rb ;

from (
  map name
    using 'token_lengths.rb' as (word string, freq int)
  from athlete
  cluster by word) t 
reduce word, freq
  using 'average_unique_lengths.rb' as (avg) ;
