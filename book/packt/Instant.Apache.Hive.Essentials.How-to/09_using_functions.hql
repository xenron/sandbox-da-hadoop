create database if not exists ch9 ;

use ch9 ;

set hive.exec.mode.local.auto=true ;

show functions ;

describe function regexp_extract ;

describe function extended regexp_extract ;

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

select
  regexp_extract(name, '(\\S+).* (\\S+)', 1),
  regexp_extract(name, '(\\S+).* (\\S+)', 2)
from athlete
limit 5 ;

select
  name,
  medal_id
from athlete
  lateral view explode(medals_won) combined_tables as medal_id
limit 5 ;

select explode(medals_won) as medal_id
from athlete limit 5 ;

select 
  country,
  avg(length(athlete)) as avg_name_length
from athlete_affiliation
group by country 
order by avg_name_length desc
limit 5 ;
