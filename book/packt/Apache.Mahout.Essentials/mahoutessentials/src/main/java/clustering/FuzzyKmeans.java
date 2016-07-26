package clustering;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.mahout.clustering.Cluster;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.conversion.InputDriver;
import org.apache.mahout.clustering.fuzzykmeans.FuzzyKMeansDriver;
import org.apache.mahout.clustering.kmeans.RandomSeedGenerator;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.common.distance.SquaredEuclideanDistanceMeasure;
import org.apache.mahout.utils.clustering.ClusterDumper;

public class FuzzyKmeans {

	private static final String DIRECTORY_CONTAINING_CONVERTED_INPUT = "data/FuzzyKmeansData";
	private static final String DIRECTORY_INPUT = "test/FuzzyKmeansTest";
	private static final String DIRECTORY_OUTPUT = "data/FuzzyKmeansOutput";
	
	public static void main(String[] args) throws Exception {
		
		 Path output = new Path(DIRECTORY_OUTPUT);
	     
	     Configuration conf = new Configuration();
	     
	     HadoopUtil.delete(conf, output);
	     run(conf, new Path(DIRECTORY_INPUT), output, new SquaredEuclideanDistanceMeasure(), 2, 0.5, 10);
		}
	     
	     public static void run(Configuration conf, Path input, Path output, DistanceMeasure measure, int k,
	   	      double convergenceDelta, int maxIterations) throws Exception {
			  
			// Preparing input
	   	    Path directoryContainingConvertedInput = new Path(output, DIRECTORY_CONTAINING_CONVERTED_INPUT);
	   	    InputDriver.runJob(input, directoryContainingConvertedInput, "org.apache.mahout.math.RandomAccessSparseVector");
	   	 
	   	    // Get initial clusters randomly 
	   	    Path clusters = new Path(output, "random-seeds");
	   	    clusters = RandomSeedGenerator.buildRandom(conf, directoryContainingConvertedInput, clusters, k, measure);
	   	    
	   	    // Run K-means with a given K
	   	    FuzzyKMeansDriver.run(conf, directoryContainingConvertedInput, clusters, output, convergenceDelta,
	   	        maxIterations,2F, true,false, 0, false);
	   	    
	  	    // read clusters and output
			FileSystem fs = FileSystem.get(conf);
		    Reader reader = new SequenceFile.Reader(fs,new Path(output, Cluster.CLUSTERED_POINTS_DIR + "/part-m-00000"), 
		      conf);
		    IntWritable key = new IntWritable();
		    WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
		  
		    while (reader.next(key, value)) {
		      System.out.println("key: " + key.toString()+ " value: "+ value.toString());
		    }
		    reader.close();
	   	    
	   	    // run ClusterDumper
	   	    Path outGlob = new Path(output, "clusters-*-final");
	   	    Path clusteredPoints = new Path(output,"clusteredPoints");
	   	
	   	    ClusterDumper clusterDumper = new ClusterDumper(outGlob, clusteredPoints);
	   	   
	   	    // Display cluster information
	   	    Map<Integer, List<WeightedPropertyVectorWritable>> clusterIDs = clusterDumper.getClusterIdToPoints();

				for (Map.Entry<Integer, List<WeightedPropertyVectorWritable>> entry : clusterIDs.entrySet())
				{
				    System.out.println(entry.getKey() + ": " + entry.getValue());
				}
			System.out.println();
	     }
}
