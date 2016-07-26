package com.packput.hadoop;

import java.io.IOException;

import java.text.NumberFormat;

import javax.tools.Tool;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrConfig;

import org.apache.solr.update.SolrIndexConfig;
import org.apache.hadoop.contrib.index.mapred.IIndexUpdater;
import org.apache.hadoop.contrib.index.mapred.IndexUpdateConfiguration;
import org.apache.hadoop.contrib.index.mapred.Shard;

import org.xml.sax.SAXException;

public class SolrIndexGenMapper
{
  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }
  
  private Configuration getJobConfiguration()
    throws Exception
  {
    Configuration conf = new Configuration();
    conf.set("fs.default.name", "hdfs://10.129.1.1:9000/");
    conf.set("mapred.job.tracker", "10.129.1.1:9001");
    //the step below is optional depending upon your hadoop version
    conf.set("hadoop.job.ugi", "hrishikesh");
    return conf;
  }

  public SolrIndexGenMapper(String solrHome, String dataDir, Path inputPath, Path indexPath, Path outputPath, int noOfServer)
    throws Exception
  {    
    int numMapTasks = 5;
    int maxSegments = 5;
    long versionNumber = -1;
    long generation = -1;
    System.setProperty("solr.solr.home", solrHome);
    System.setProperty("solr.core.dataDir", dataDir);

    SolrConfig solrConfig = new SolrConfig();
    Configuration conf = getJobConfiguration();
    FileSystem fs = FileSystem.get(conf);

    if (fs.exists(outputPath))
      fs.delete(outputPath, true);
    if (fs.exists(indexPath))
      fs.delete(indexPath, true);

    for (int noShards = 0; noShards < noOfServer; noShards++)
    {
      //Set initial parameters
      IndexUpdateConfiguration iconf = new IndexUpdateConfiguration(conf);
      iconf.setIndexInputFormatClass(SolrXMLDocInputFormat.class);
      iconf.setLocalAnalysisClass(SolrLocalAnalysis.class);
      //configure the indexing for SOlr
      SolrIndexConfig solrIndexConf = solrConfig.mainIndexConfig;
      if (solrIndexConf.maxFieldLength != -1)
        iconf.setIndexMaxFieldLength(solrIndexConf.maxFieldLength);
      iconf.setIndexUseCompoundFile(solrIndexConf.useCompoundFile);
      iconf.setIndexMaxNumSegments(maxSegments);
            
      //initialize array
      Shard[] shards = new Shard[numShards];
      for (int j = 0; j < shards.length; j++)
      {
        Path path = new Path(indexPath, NUMBER_FORMAT.format(j));
        shards[j] = new Shard(versionNumber, path.toString(), generation);
      }
      //An implementation of an index updater interface which creates a Map/Reduce job configuration and run the 
      //Map/Reduce job to analyze documents and update Lucene instances in parallel.
      IIndexUpdater updater = new SolrIndexUpdater();
      updater.run(conf, new Path[]
          { inputPath }, outputPath, numMapTasks, shards);
      // verify the done files
      Path[] doneFileNames = new Path[shards.length];
      int count = 0;
      FileStatus[] fileStatus = fs.listStatus(outputPath);
      for (int i = 0; i < fileStatus.length; i++)
      {
        FileStatus[] doneFiles = fs.listStatus(fileStatus[i].getPath());
        for (int j = 0; j < doneFiles.length; j++)
        {
          doneFileNames[count++] = doneFiles[j].getPath();
        }
      }      
      //you can move your shards here to different servers
      
    }
  }
}
