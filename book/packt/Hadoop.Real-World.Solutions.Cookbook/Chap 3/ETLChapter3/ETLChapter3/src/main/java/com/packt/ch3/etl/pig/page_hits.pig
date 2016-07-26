nobots_weblogs = LOAD '/user/jowens/nobots/part*' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

page_groups = GROUP nobots_weblogs BY page;

page_hits = FOREACH page_groups GENERATE group, COUNT(nobots_weblogs);

STORE page_hits INTO '/user/jowens/page_hits';
