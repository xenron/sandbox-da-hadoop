/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.recipe.graph;

import java.io.IOException;
import java.util.List;

import microbook.format.BuyerRecord;
import microbook.format.ItemSalesDataFormat;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <p>
 * This program calculates the number of items brought by each buyer
 * </p>
 * 
 * @author Srinath Perera (srinath@wso2.com)
 */
public class BuyingFrequencyAnalyzer {

    /**
   * 
   */
    public static class BuyingFrequencyMapper extends Mapper<Object, Text, Text, IntWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<BuyerRecord> records = BuyerRecord.parseAItemLine(value.toString());
            for (BuyerRecord record : records) {
                context.write(new Text(record.customerID), new IntWritable(record.itemsBrought.size()));
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
    public static class BuyingFrequencyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
                InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            System.out.println(key + "= "+ sum);
            context.write(key, new IntWritable(sum));
        }
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

        Job job = new Job(conf, "BuyingFrequencyAnalyzer");
        job.setJarByClass(BuyingFrequencyAnalyzer.class);
        job.setMapperClass(BuyingFrequencyMapper.class);
        job.setReducerClass(BuyingFrequencyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(ItemSalesDataFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
