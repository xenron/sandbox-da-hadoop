create table if not exists top_athletes(
  name string,
  num_medals int) ;

insert overwrite table top_athletes
select name, size(medals_won) as num_medals
from athlete
where size(medals_won) >= ${threshold}
order by num_medals desc, name asc ;
