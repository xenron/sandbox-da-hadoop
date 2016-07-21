/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.set;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * <p>
 * This program calculates the number of items brought by each buyer
 * </p>
 * 
 * @author Srinath Perera (srinath@wso2.com)
 */
public class BuyersSetDifference {
    private static Pattern parsingPattern = Pattern.compile("([^\\s]+)\\s+([^\\s]+)");

    public static class AMapper extends Mapper<Object, Text, Text, IntWritable> {

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    String currentFile =  ((FileSplit)context.getInputSplit()).getPath().getName();
    
    Matcher matcher = parsingPattern.matcher(value.toString());
    if (matcher.find()) {
        String propName = matcher.group(1);
        String propValue = matcher.group(2);
        if(currentFile.equals("first100ItemBuyers.data")){
            context.write(new Text(propName), new IntWritable(1));
        }else if(currentFile.equals("mostFrequentBuyers.data")){
            int count = Integer.parseInt(propValue);
            if(count > 100){
                context.write(new Text(propName), new IntWritable(2));                        
            }
        }else{
            throw new IOException("Unexpected file "+ currentFile); 
        }
    } else {
        System.out.println(currentFile + ":Unprocessed Line " + value);
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
    public static class AReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
        InterruptedException {
    boolean has1 = false;
    boolean has2 = false;
    System.out.print(key + "=");
    for (IntWritable val : values) {
        switch(val.get()){
            case 1:
                has1 = true;
                break;
            case 2:
                has2 = true;
                break;
        }
        System.out.println(val);
    }
    if(has1 && !has2){
        context.write(key, new IntWritable(1));    
    }
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

        Job job = new Job(conf, "BuyersSetDifference");
        job.setJarByClass(BuyersSetDifference.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
