/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.search;

import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeSet;

import microbook.format.BuyerRecord;
import microbook.format.ItemSalesDataFormat;
import microbook.format.BuyerRecord.ItemData;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Srinath Perera (srinath@wso2.com)
 */
public class TitleInvertedIndexGenerator {

    /**
   * 
   */
    public static class AMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<BuyerRecord> records = BuyerRecord.parseAItemLine(value.toString());
            for (BuyerRecord record : records) {
                for(ItemData item: record.itemsBrought){
                    StringTokenizer itr = new StringTokenizer(item.title);
                    while (itr.hasMoreTokens()) {
                        String token = itr.nextToken().replaceAll("[^A-z0-9]", "");
                        if(token.length() > 0){
                            context.write(new Text(token), new Text(pad(String.valueOf(item.salesrank))+ "#" +item.itemID));    
                        }
                    }
                }
            }
        }
    }

    /**
     * <p>
     * Reduce function receives all the values that has the same key as the
     * input, and it output the key and the number of occurrences of the key as
     * the output.
     * </p>
     */
    public static class AReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            TreeSet<String> set = new TreeSet<String>();
            for (Text valtemp : values) {
                set.add(valtemp.toString());
            }

            StringBuffer buf = new StringBuffer();
            for (String val : set) {
                buf.append(val).append(",");
            }
            context.write(key, new Text(buf.toString()));
        }
    }

    
    public static String pad(String name){
        StringBuffer buf = new StringBuffer();
        
        for(int i = name.length(); i< 8;i++){
            buf.append("0");
        }
        buf.append(name);
        return buf.toString();
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "TitleInvertedIndexGenerator");
        job.setJarByClass(TitleInvertedIndexGenerator.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(ItemSalesDataFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
