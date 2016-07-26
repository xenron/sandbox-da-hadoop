/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.graph;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import microbook.format.BuyerRecord;
import microbook.format.BuyerRecord.ItemData;
import microbook.format.ItemSalesDataFormat;
import microbook.graph.SimilarItemsFinder.Color;
import microbook.graph.SimilarItemsFinder.GNode;

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
 * ID    EDGES|DISTANCE_FROM_SOURCE|COLOR|
 */
public class SimilarItemsPreprocessor {
    private static int recordCount = 0; 
    
    public static class AMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            List<BuyerRecord> records = BuyerRecord.parseAItemLine(value.toString());
            for (BuyerRecord record : records) {
                for(ItemData itemData: record.itemsBrought){
                    if(itemData.similarItems.size() > 0 && itemData.salesrank < 1000){
                        GNode node = new GNode(itemData.itemID, itemData.similarItems.toArray(new String[0])); 
                        
                        if(itemData.itemID.equals("0006176909")){
                            node.color = Color.Gray;
                        }
                        
                        context.write(new Text(itemData.itemID), new Text(node.toString()));
                        recordCount++;
                        if(recordCount %1000 == 0){
                            System.out.println(recordCount + "records founds");
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
            GNode firstNode = null;
            Set<String> list = new HashSet<String>(); 
            for(Text value: values){
                GNode node = new GNode(key.toString(), value.toString());
                for(String e: node.edges){
                    list.add(e);
                }
                if(firstNode == null){
                    firstNode = node; 
                }
            }
            firstNode.edges = list.toArray(new String[0]);
            context.write(key, new Text(firstNode.toString()));
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

        Job job = new Job(conf, "SimilarItemsPreprocessor");
        job.setJarByClass(SimilarItemsPreprocessor.class);
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
