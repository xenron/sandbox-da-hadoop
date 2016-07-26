/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.crosscorrelation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import microbook.format.BuyerRecord;
import microbook.format.ItemSalesDataFormat;
import microbook.graph.SimilarItemsFinder;

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
 * <p>
 * This program calculates the number of items brought by each buyer
 * </p>
 * 
 * @author Srinath Perera (srinath@wso2.com)
 */
public class CustomerCorrleationFinder {
    private static Pattern parsingPattern = Pattern.compile("([^\\s]+)\\s+([^\\s]+)");

    public static class AMapper extends Mapper<Object, Text, Text, Text> {

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    List<BuyerRecord> records = BuyerRecord.parseAItemLine(value.toString());
    List<String> customers = new ArrayList<String>();
    
    for(BuyerRecord record: records){
        customers.add(record.customerID);
    }
    
    for(int i =0;i< records.size();i++){
        StringBuffer buf = new StringBuffer();
        int index = 0;
        for(String customer:customers){
            if(index != i){
                buf.append(customer).append(",");                        
            }
        }
        context.write(new Text(customers.get(i)), new Text(buf.toString()));
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
    Set<String> customerSet = new HashSet<String>();
    for(Text text: values){
        String[] split = text.toString().split(",");
        for(String token:split){
            customerSet.add(token);
        }
    }

    StringBuffer buf = new StringBuffer();
    for(String customer: customerSet){
        if(customer.compareTo(key.toString()) < 0){
            buf.append(customer).append(",");    
        }
    }
    buf.append("|").append(Integer.MAX_VALUE).append("|").append(SimilarItemsFinder.Color.White);
    context.write(new Text(key), new Text(buf.toString()));
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

        Job job = new Job(conf, "BuyerRecordJoinJob");
        job.setJarByClass(CustomerCorrleationFinder.class);
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
