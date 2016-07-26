CREATE WRITABLE EXTERNAL TABLE weblogs(
    md5             text,
    url             text,
    request_date    date,
    request_time    time,
    ip              inet
)
LOCATION ('gphdfs://<NAMENODE_HOST>:<NAMENODE_PORT>/data/weblogs/weblog_entries.txt')
FORMAT 'TEXT' (DELIMITER '\t');
