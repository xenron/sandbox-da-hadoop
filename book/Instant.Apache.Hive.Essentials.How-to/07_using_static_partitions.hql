create database if not exists ch7 ;

use ch7 ;

set hive.exec.mode.local.auto=true ;

create external table if not exists athlete(
  name string,
  id string,
  demonstration_events_competed_in array<string>,
  demonstration_medals_won array<string>,
  country array<string>,
  medals_won array<string>
)
partitioned by (part_key string)
row format delimited
fields terminated by '\t'
collection items terminated by ',' ;

alter table athlete add if not exists partition(part_key = 'a') location '/data/athlete/part_key=a/' ;
alter table athlete add if not exists partition(part_key = 'b') location '/data/athlete/part_key=b/' ;
alter table athlete add if not exists partition(part_key = 'c') location '/data/athlete/part_key=c/' ;

show partitions athlete ;

create external table if not exists athlete_affiliation(
  name string,
  id string,
  athlete string,
  country string,
  olympics string,
  sport string
)
partitioned by (part_key string)
row format delimited
fields terminated by '\t'
collection items terminated by ',' ;

alter table athlete_affiliation add if not exists partition(part_key = 'a') location '/data/athlete_affiliation/part_key=a/' ;
alter table athlete_affiliation add if not exists partition(part_key = 'b') location '/data/athlete_affiliation/part_key=b/' ;
alter table athlete_affiliation add if not exists partition(part_key = 'c') location '/data/athlete_affiliation/part_key=c/' ;

explain dependency
select * from athlete where part_key = 'a' ;

explain dependency
select
  l.name,
  size(l.medals_won),
  r.country,
  collect_set(r.olympics)
from athlete l
  join athlete_affiliation r
  on l.name = r.athlete
  and l.part_key = 'a'
  and r.part_key = 'a'
group by l.name, size(l.medals_won), r.country
order by name asc ;
