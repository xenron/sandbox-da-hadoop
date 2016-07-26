--读取数据
data =LOAD '/user/qos/dataguru/lbs.data' USING PigStorage('|') AS (imsi:chararray,time:chararray,loc:chararray);

--注册 jars
REGISTER piggybank-0.12.0.jar;
REGISTER joda-time-1.6.jar;
REGISTER datafu-1.0.0.jar;

DEFINE CustomFormatToISO org.apache.pig.piggybank.evaluation.datetime.convert.CustomFormatToISO();
toISO = FOREACH data GENERATE imsi,CustomFormatToISO( SUBSTRING(time,0,13), 'YYYY-MM-dd HH') AS  time:chararray,loc;

--数据分组
grp =GROUP toISO BY imsi;

--获取连续数据
DEFINE MarkovPairs datafu.pig.stats.MarkovPairs();
pairs =FOREACH grp { sorted =ORDER toISO BY time;
  pair =MarkovPairs(sorted);
  GENERATE FLATTEN(pair) AS (data:tuple(imsi,time,loc),
  next:tuple(imsi,time,loc));
};
--展开数据
prj =FOREACH pairs GENERATE data.imsi AS imsi,data.time AS time,next.time AS next_time,data.loc AS loc,next.loc AS next_loc;
--过滤不是连续的数据
DEFINE ISOHoursBetween org.apache.pig.piggybank.evaluation.datetime.diff.ISOHoursBetween();
flt =FILTER prj BY ISOHoursBetween(next_time,time)<12L;
--计算每一个位置的总数
total_count =FOREACH (GROUP flt BY loc) GENERATE group AS loc,COUNT(flt) AS total;
--计算每一对位置的总数
pairs_count =FOREACH (GROUP flt BY (loc,next_loc)) GENERATE FLATTEN(group) AS (loc,next_loc),COUNT(flt) AS cnt;
--表链接
jnd =JOIN pairs_count BY loc,total_count BY loc USING 'replicated';
--计算概率
prob =FOREACH jnd GENERATE pairs_count::loc AS loc,pairs_count::next_loc AS next_loc,(double)cnt/(double)total AS probability;
--只保留概率最大的三个地方
top3 =FOREACH (GROUP prob BY loc) {
  sorted =ORDER prob BY probability DESC;
  top =LIMIT sorted 3;
  GENERATE FLATTEN(top);
}
DUMP top3;
