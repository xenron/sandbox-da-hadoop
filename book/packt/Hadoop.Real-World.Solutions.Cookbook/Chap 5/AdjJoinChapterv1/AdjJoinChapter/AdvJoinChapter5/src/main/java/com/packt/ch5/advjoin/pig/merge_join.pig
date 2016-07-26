
nobots_weblogs = LOAD '/user/hadoop/sorted_apache_nobots_tsv.txt' AS (ip: chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);
ip_country_tbl = LOAD '/user/hadoop/sorted_nobots_ip_country_tsv.txt' AS (ip:chararray, country:chararray);

weblog_country_jnd = JOIN nobots_weblogs BY ip, ip_country_tbl BY ip USING 'merge';

cleaned = FOREACH weblog_country_jnd GENERATE ip_country_tbl::ip, country, timestamp, page, http_status, payload_size, useragent;
STORE cleaned  INTO '/user/hadoop/weblog_country_jnd_merge';

