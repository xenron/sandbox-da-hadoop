create database if not exists ch4;

use ch4;

set hive.exec.mode.local.auto = true ;

set hive.mapjoin.localtask.max.memory.usage = 0.99 ;

set hive.enforce.bucketing = true ;

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

create table if not exists athlete_bucketed(
  name string,
  id string,
  demonstration_events_competed_in array<string>,
  demonstration_medals_won array<string>,
  country array<string>,
  medals_won array<string>)
clustered by (name) into 2 buckets ;

insert overwrite table athlete_bucketed
select * from athlete ;

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

create table if not exists athlete_affiliation_bucketed(
  name string,
  id string,
  athlete string,
  country string,
  olympics string,
  sport string)
clustered by (athlete) into 4 buckets ;

insert overwrite table athlete_affiliation_bucketed
select * from athlete_affiliation ;

set hive.optimize.bucketmapjoin = true ;

set hive.optimize.bucketmapjoin.sortedmerge = true ;

set hive.input.format =
 org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;

select
  l.name,
  size(l.medals_won),
  r.country,
  collect_set(r.olympics)
from athlete_bucketed l
  join athlete_affiliation_bucketed r 
  on l.name = r.athlete
group by l.name, size(l.medals_won), r.country
order by name asc
limit 5 ;
