import java.io.*;

import org.apache.commons.logging.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.mapreduce.*;
import org.bson.*;

import com.mongodb.hadoop.*;
import com.mongodb.hadoop.util.*;

public class ImportWeblogsFromMongo {
	
	private static final Log log = LogFactory.getLog(ImportWeblogsFromMongo.class);
	
	public static class ReadWeblogsFromMongo extends Mapper<Object, BSONObject, Text, Text>{
		
		public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException{
			
			System.out.println("Key: " + key);
			System.out.println("Value: " + value);
			
			String md5 = value.get("md5").toString();
			String url = value.get("url").toString();
			String date = value.get("date").toString();
			String time = value.get("time").toString();
			String ip = value.get("ip").toString();
			String output = "\t" + url + "\t" + date + "\t" + time + "\t" + ip;
			context.write( new Text(md5), new Text(output));
		}
	}
	
	public static void main(String[] args) throws Exception{
		
		final Configuration conf = new Configuration();
		MongoConfigUtil.setInputURI(conf,
mongodb://<HOST>:<PORT>/test.weblogs");
		
		MongoConfigUtil.setCreateInputSplits(conf, false);
		System.out.println("Configuration: " + conf);
		
		final Job job = new Job(conf, "Mongo Import");
		
		Path out = new Path("/data/weblogs/mongo_import");
		FileOutputFormat.setOutputPath(job, out);
		
		job.setJarByClass(ImportWeblogsFromMongo.class);
		job.setMapperClass(ReadWeblogsFromMongo.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setInputFormatClass(MongoInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setNumReduceTasks(0);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1 );
		
	}

}
