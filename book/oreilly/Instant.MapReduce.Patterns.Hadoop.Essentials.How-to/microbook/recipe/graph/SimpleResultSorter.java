package microbook.recipe.graph;


import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class SimpleResultSorter {

    private static Pattern parsingPattern = Pattern
            .compile("([^\\s]+)\\s+([^\\s]+)");

    public static String outprefix = "";
    
    public static boolean sortByValue = true; 

    public static class SimpleKeyMapper extends
            Mapper<Object, Text, IntWritable, Text> {

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
                Matcher matcher = parsingPattern.matcher(value.toString());
                if (matcher.find()) {
                    String propName = matcher.group(1);
                    String propValue = matcher.group(2);
                        context.write(new IntWritable(-1*Integer.valueOf(propValue)), new Text(propName));    
                } else {
                    System.out.println("Unprocessed Line " + value);
                }
        }
    }

    public static class SimpleKeyReducer extends
            Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text valtemp : values) {
                IntWritable recordFrequency = new IntWritable(-1*key.get()); 
                context.write(valtemp, recordFrequency);
                System.out.println(valtemp + "=" + recordFrequency);
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
          
      Job job = new Job(conf, "word count");
      job.setJarByClass(SimpleResultSorter.class);
      job.setMapperClass(SimpleKeyMapper.class);
      job.setReducerClass(SimpleKeyReducer.class);
      job.setMapOutputKeyClass(IntWritable.class);
      job.setMapOutputValueClass(Text.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
      FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
