CREATE DATABASE IF NOT EXISTS ${dbName};

use ${dbName};

CREATE TABLE IF NOT EXISTS `tweets_hcat` (
created_at string,
tweet_id_str string,
text string,
in_reply_to string,
is_retweeted string,
user_id string,
place_id string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\u0001'
STORED AS SEQUENCEFILE;

CREATE  TABLE IF NOT EXISTS `places_hcat`(
  `place_id` string,
  `country_code` string,
  `country` string,
  `name` string,
  `full_name` string,
  `place_type` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE;

CREATE  TABLE IF NOT EXISTS `users_hcat`(
  `created_at` string,
  `user_id` string,
  `location` string,
  `name` string,
  `description` string,
  `followers_count` bigint,
  `friends_count` bigint,
  `favourites_count` bigint,
  `screen_name` string,
  `listed_count` bigint)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
STORED AS SEQUENCEFILE;
