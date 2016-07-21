/**
 * Following sample is adopted from original wordcount sample from 
 * http://wiki.apache.org/hadoop/WordCount. 
 */
package microbook.kmean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileSystem;
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
public class KmeanCluster {

    private static Pattern parsingPattern = Pattern.compile("([^\\s]+)\\s+([^\\s]+)");

    public static class AMapper extends Mapper<Object, Text, Text, Text> {
        
        private double[][] centroids = new double[0][];
        
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            
            DataInputStream d = new DataInputStream(fs.open(new Path("/data/kmeans/clusters.data")));
            BufferedReader reader = new BufferedReader(new InputStreamReader(d));
            String line = null;
            int index = 0;
            while ((line = reader.readLine()) != null){
                String[] tokens = line.split(",");
                double lat = Double.parseDouble(tokens[0]);
                double lon = Double.parseDouble(tokens[1]);
                centroids[index] = new double[]{lat,lon};
                index++;         
            }
            reader.close();
            
            fs.rename(new Path("/data/kmeans/clusters.data"), new Path("/data/kmeans/clusters.data"+System.currentTimeMillis()));
            
            // read centrioids
            super.setup(context);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            //read clusters
            //assign users to clusters 
            Matcher matcher = parsingPattern.matcher(value.toString());
            if (matcher.find()) {
                String propName = matcher.group(1);
                String propValue = matcher.group(2);
                String[] tokens = propValue.split(",");
                double lat = Double.parseDouble(tokens[0]);
                double lon = Double.parseDouble(tokens[1]);
                
                int minCentroidIndex = -1; 
                double minDistance = Double.MAX_VALUE;
                int index = 0;
                for(double[] point: centroids){
                    double distance = Math.sqrt(Math.pow(point[0] -lat, 2) + Math.pow(point[1] -lon, 2));
                    if(distance < minDistance){
                        minDistance = distance;
                        minCentroidIndex = index;
                    }
                    index++;
                }
                String centriod = centroids[minCentroidIndex] + "," + centroids[minCentroidIndex];
                String point = lat +"," + lon;
                context.write(new Text(centriod), new Text(point));
            } else {
                System.out.println(":Unprocessed Line " + value);
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
            context.write(key, key);
            //recalcualte clusters 
            double totLat = 0; 
            double totLon = 0;
            int count = 0;
            
            for(Text text: values){
                String[] tokens = text.toString().split(",");
                double lat = Double.parseDouble(tokens[0]);
                double lon = Double.parseDouble(tokens[1]);
                totLat = totLat + lat; 
                totLon = totLon + lon; 
                count++;
            }
            
            String centroid = (totLat/count) + "," + (totLon/count);
            
            //print them out
            for(Text token: values){
                context.write(new Text(token), new Text(centroid));

            }
            FileSystem fs = FileSystem.get(context.getConfiguration());

            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fs.create(new Path("/data/kmeans/clusters.data"), true)));
            bw.write(centroid);bw.write("\n");
            bw.close();

        }
    }
    
    public static int submitJob(String[] args, String inputDir, String outputDir) throws Exception{
        JobConf conf = new JobConf();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "KmeanCluster");
        job.setJarByClass(KmeanCluster.class);
        job.setMapperClass(AMapper.class);
        job.setReducerClass(AReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));
        job.waitForCompletion(true);
        
        FileSystem fs = FileSystem.get(job.getConfiguration());

        DataInputStream d = new DataInputStream(fs.open(new Path("/data/kmeans/clusters.data")));
        BufferedReader reader = new BufferedReader(new InputStreamReader(d));
        String line = null;
        StringBuffer centroidsAsStr = new StringBuffer(); 
        while ((line = reader.readLine()) != null){
            if(line.trim().length() > 0){
                centroidsAsStr.append(line); 
            }
        }
        reader.close();
        return centroidsAsStr.hashCode();
    }
    

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        String inputDir = args[0];
        String outputDir = args[1];
        
        //read and pick random centroids
        //upload the centroids 
        boolean toContinue = true;
        int index = 0;
        int centroidsHash = -1; 
        
        while(toContinue){
            String jobInputDir;
            if(index == 0){
                jobInputDir = inputDir;
            }else{
                jobInputDir = outputDir + "/job"+(index-1);
            }
            System.out.println("running "+ index + " iteration");
            int hash = submitJob(args, jobInputDir, outputDir+"/job"+index);
            toContinue = (centroidsHash == -1) || (hash != centroidsHash);
            index++;
        }
    }
}
