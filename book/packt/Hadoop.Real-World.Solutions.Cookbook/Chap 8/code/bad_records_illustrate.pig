weblogs = load '/data/weblogs/weblog_entries_bad_records.txt' 
	as (md5:chararray, url:chararray, date:chararray, time:chararray, ip:chararray);

bad		= filter weblogs by not 
(ip matches '^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.([01]?\\d\\d?|2[0-4]\\d|25[0-5])$');

illustrate bad;
