nobots_weblogs = LOAD '/user/jowens/nobots/part*' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

ordered_weblogs = ORDER nobots BY timestamp;

STORE ordered_weblogs INTO '/user/jowens/ordered_weblogs';