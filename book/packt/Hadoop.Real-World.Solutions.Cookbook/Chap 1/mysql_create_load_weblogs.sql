CREATE DATABASE logs;

USE logs;
CREATE TABLE weblogs(
    md5             VARCHAR(32),
    url             VARCHAR(64),
    request_date    DATE,
    request_time    TIME,
    ip              VARCHAR(15)
);
LOAD DATA INFILE '/path/weblog_entries.txt' INTO TABLE weblogs
FIELDS TERMINATED BY '\t' LINES TERMINATED BY '\r\n';
