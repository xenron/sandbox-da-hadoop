register /path/to/mongo-hadoop/mongo-2.8.0.jar
register /path/to/mongo-hadoop/core/target/mongo-hadoop-core-1.0.0.jar
register /path/to/mongo-hadoop/pig/target/mongo-hadoop-pig-1.0.0.jar

define MongoStorage com.mongodb.hadoop.pig.MongoStorage();

weblogs = load '/data/weblogs/weblog_entries.txt' as 
                (md5:chararray, url:chararry, date:chararray, time:chararray, ip:chararray);

store weblogs into 'mongodb://<HOST>:<PORT>/test.weblogs_from_pig' using MongoStorage();
