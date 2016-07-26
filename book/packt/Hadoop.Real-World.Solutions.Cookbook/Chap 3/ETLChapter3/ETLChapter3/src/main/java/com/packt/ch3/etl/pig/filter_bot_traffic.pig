set mapred.cache.files '/user/hadoop/blacklist.txt#blacklist';
set mapred.create.symlink 'yes';

register ETLChapter3-1.0-SNAPSHOT.jar;

all_weblogs = LOAD '/user/jowens/tsv/part*' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

nobots_weblogs = FILTER all_weblogs BY NOT com.packt.ch3.etl.pig.IsUseragentBot(useragent);

STORE nobots_weblogs INTO '/user/jowens/nobots';
