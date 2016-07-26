/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.graph;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
public class SimilarItemsFinder {
    public static enum Color{White, Gray,Black, Red};

    public static class GNode{
        public String id;
        public String[] edges;
        public int minDistance = Integer.MAX_VALUE;
        public Color color = Color.White; 
        
        public String toString(){
            StringBuffer buf = new StringBuffer(); 
            if(edges != null){
                for(String edge:edges){
                    if(edge.trim().length() > 0){
                        buf.append(edge).append(",");                    
                    }
                }
            }
            return buf.append("|").append(minDistance).append("|").append(color.toString()).toString();
        }

        public GNode(String id, String[] edges){
            this.id = id;
            this.edges = edges;
        }
        
        public GNode(String id, String fromVal){
            this.id = id; 
            String[] tokens = fromVal.split("\\|");
            this.edges = tokens[0].split(",");
            minDistance = Integer.parseInt(tokens[1]);
            color = Color.valueOf(tokens[2]);
        }
        
    }
    
    private static Pattern parsingPattern = Pattern
    .compile("([^\\s]+)\\s+([^\\s]+)");

    /**
     * Mapper process the Gray nodes. Others, it emits as is. 
     */
    public static class AMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            Matcher matcher = parsingPattern.matcher(value.toString());
            if (matcher.find()) {
                String id = matcher.group(1);
                String val = matcher.group(2);
                
                GNode node = new GNode(id, val); 
                if(node.color == Color.Gray){
                    node.color = Color.Black;
                    context.write(new Text(id), new Text(node.toString()));
                    for(String e: node.edges){
                        GNode nNode = new GNode(e, (String[])null);
                        nNode.minDistance = node.minDistance+1;
                        nNode.color = Color.Red;
                        context.write(new Text(e), new Text(nNode.toString()));
                    }
                }else{
                    context.write(new Text(id), new Text(val)); 
                }
            } else {
                System.out.println("Unprocessed Line " + value);
            }
        }
    }

    /**
     * <p>
     * Reduce Merge distance of all nodes
     * </p>
     */
    public static class AReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
                InterruptedException {
            GNode originalNode =  null; 
            boolean hasRedNodes = false;
            int minDistance = Integer.MAX_VALUE;
            for(Text val: values){
                GNode node = new GNode(key.toString(), val.toString());
                if(node.color == Color.Black || node.color == Color.White){
                    originalNode = node;
                }else if(node.color == Color.Red){
                    hasRedNodes = true;
                }
                if(minDistance > node.minDistance){
                    minDistance = node.minDistance; 
                }
            }
            if(originalNode != null){
                originalNode.minDistance = minDistance;
                if(originalNode.color == Color.White && hasRedNodes){
                    originalNode.color = Color.Gray;
                }
                context.write(key, new Text(originalNode.toString()));
            }
        }
    }
    
    public static void submit(String outputFolder, String inputFolder, String[] args) throws Exception{
        JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "SimilarItemsFinder");
        job.setJarByClass(SimilarItemsFinder.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputFolder));
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));
        job.waitForCompletion(true);
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        boolean continueProcessing = true;
        Pattern parsingPattern = Pattern.compile("([^\\s]+)\\s+([^\\s]+)");

        String inputFile = args[0];
        File baseDir = new File(args[1]);
        if(!baseDir.exists()){
            baseDir.mkdir();
        }
        
        int count = 0;
        while(continueProcessing){
            String outputDir = new File(baseDir, String.valueOf(count)).getAbsolutePath();  
            submit(outputDir, args[0], args);
            
            BufferedReader br = new BufferedReader(new FileReader(outputDir+"/part-r-00000"));
            String line = br.readLine();
            while (line != null) {
                Matcher matcher = parsingPattern.matcher(line);
                if (matcher.find()) {
                    String key = matcher.group(1);
                    String value = matcher.group(2);
                    GNode node = new GNode(key, value);
                    
                    if(node.color == Color.Gray){
                        continueProcessing = true;
                        break;
                    }

                }
            }
            br.close();
            continueProcessing = false;
        }
    }
}
