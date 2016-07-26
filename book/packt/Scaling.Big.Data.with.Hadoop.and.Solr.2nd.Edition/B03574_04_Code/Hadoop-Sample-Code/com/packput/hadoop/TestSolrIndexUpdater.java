package com.packput.hadoop;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.StringWriter;
import java.text.NumberFormat;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.contrib.index.lucene.FileSystemDirectory;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.MiniMRCluster;
import org.apache.lucene.index.IndexFileNameFilter;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.util.XML;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.schema.IndexSchema;
import org.apache.solr.update.SolrIndexConfig;
import org.apache.solr.util.AbstractSolrTestCase.Doc;
import org.apache.solr.TestDistributedSearch;

import org.apache.hadoop.contrib.index.mapred.IIndexUpdater;
import org.apache.hadoop.contrib.index.mapred.IndexUpdateConfiguration;
import org.apache.hadoop.contrib.index.mapred.Shard;

public class TestSolrIndexUpdater{

  private static final NumberFormat NUMBER_FORMAT = NumberFormat.getInstance();
  static {
    NUMBER_FORMAT.setMinimumIntegerDigits(5);
    NUMBER_FORMAT.setGroupingUsed(false);
  }

  // however, "we only allow 0 or 1 reducer in local mode" - from
  // LocalJobRunner
  private Configuration conf;
  private Path inputPath = new Path("/myexample");
  private Path outputPath = new Path("/myoutput");
  private Path indexPath = new Path("/myindex");
  private int numMapTasks = 5;

  private int numDataNodes = 3;
  private int numTaskTrackers = 3;

  private FileSystem fs;
  private MiniDFSCluster dfsCluster;
  private MiniMRCluster mrCluster;

  public TestSolrIndexUpdater() throws IOException {
    super();
    if (System.getProperty("hadoop.log.dir") == null) {
      String base = new File(".").getPath(); // getAbsolutePath();
      System.setProperty("hadoop.log.dir", new Path(base).toString() + "/logs");
    }
    conf = new Configuration();
  }

  
  public void setUp() throws Exception {
    try {
      dfsCluster =
          new MiniDFSCluster(conf, numDataNodes, true, (String[]) null);

      fs = dfsCluster.getFileSystem();
      if (fs.exists(inputPath)) {
        fs.delete(inputPath, true);
      }
      int n = 1;
      createXMLdoc(fs, inputPath, n++, id,1, i1, 100,t1,"now is the time for all good men"
          ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
      createXMLdoc(fs, inputPath, n++, id,2, i1, 50 ,t1,"to come to the aid of their country.");
      createXMLdoc(fs, inputPath, n++, id,3, i1, 2 ,t1,"how now brown cow");
      createXMLdoc(fs, inputPath, n++, id,4, i1, -100 ,t1,"the quick fox jumped over the lazy dog");
      createXMLdoc(fs, inputPath, n++, id,5, i1, 500 ,t1,"the quick fox jumped way over the lazy dog");
      createXMLdoc(fs, inputPath, n++, id,6, i1, -600 ,t1,"humpty dumpy sat on a wall");
      createXMLdoc(fs, inputPath, n++, id,7, i1, 123 ,t1,"humpty dumpy had a great fall");
      createXMLdoc(fs, inputPath, n++, id,8, i1, 876 ,t1,"all the kings horses and all the kings men");
      createXMLdoc(fs, inputPath, n++, id,9, i1, 7 ,t1,"couldn't put humpty together again");
      createXMLdoc(fs, inputPath, n++, id,10, i1, 4321 ,t1,"this too shall pass");
      createXMLdoc(fs, inputPath, n++, id,11, i1, -987 ,t1,"An eye for eye only ends up making the whole world blind.");
      createXMLdoc(fs, inputPath, n++, id,12, i1, 379 ,t1,"Great works are performed, not by strength, but by perseverance.");
      createXMLdoc(fs, inputPath, n++, id,13, i1, 232 ,t1,"no eggs on wall, lesson learned", oddField, "odd man out");
      createXMLdoc(fs, inputPath, n++, id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
      createXMLdoc(fs, inputPath, n++, id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
      createXMLdoc(fs, inputPath, n++, id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
      String[] vals = new String[100];
      for (int i = 0; i < 100; i++) {
        vals[i] = "test " + i;
      }
      createXMLdoc(fs, inputPath, n++, id, 17, "SubjectTerms_mfacet", vals);

      if (fs.exists(outputPath)) {
        // do not create, mapred will create
        fs.delete(outputPath, true);
      }

      if (fs.exists(indexPath)) {
        fs.delete(indexPath, true);
      }

      mrCluster =
          new MiniMRCluster(numTaskTrackers, fs.getUri().toString(), 1);

    } catch (IOException e) {
      if (dfsCluster != null) {
        dfsCluster.shutdown();
        dfsCluster = null;
      }

      if (fs != null) {
        fs.close();
        fs = null;
      }

      if (mrCluster != null) {
        mrCluster.shutdown();
        mrCluster = null;
      }

      throw e;
    }

  }


  public void tearDown() throws Exception {
    if (dfsCluster != null) {
      dfsCluster.shutdown();
      dfsCluster = null;
    }

    if (fs != null) {
      fs.close();
      fs = null;
    }

    if (mrCluster != null) {
      mrCluster.shutdown();
      mrCluster = null;
    }
  }

  public void testDistribSearch() throws Exception {
    
    SolrConfig solrConfig = new SolrConfig();
    for (int nServers = 1; nServers < 4; nServers++) {
      Shard[] shards = buildShards(nServers, solrConfig);
      copyShardFiles(shards);
      createServers(nServers);
      doTest();
    }
  }
  
  protected void del(String q) throws Exception {
    // only delete on controlClient
    controlClient.deleteByQuery(q);
  }

  @Override
  protected void index(Object... fields) throws Exception {
    // only index on controlClient
    SolrInputDocument doc = new SolrInputDocument();
    for (int i = 0; i < fields.length; i += 2) {
      doc.addField((String) (fields[i]), fields[i + 1]);
    }
    controlClient.add(doc);
  }

  @Override
  public void doTest() throws Exception {
    // same as in TestDistributedSearch except no index_specific tests
    del("*:*");
    index(id,1, i1, 100,t1,"now is the time for all good men"
            ,"foo_f", 1.414f, "foo_b", "true", "foo_d", 1.414d);
    index(id,2, i1, 50 ,t1,"to come to the aid of their country.");
    index(id,3, i1, 2 ,t1,"how now brown cow");
    index(id,4, i1, -100 ,t1,"the quick fox jumped over the lazy dog");
    index(id,5, i1, 500 ,t1,"the quick fox jumped way over the lazy dog");
    index(id,6, i1, -600 ,t1,"humpty dumpy sat on a wall");
    index(id,7, i1, 123 ,t1,"humpty dumpy had a great fall");
    index(id,8, i1, 876 ,t1,"all the kings horses and all the kings men");
    index(id,9, i1, 7 ,t1,"couldn't put humpty together again");
    index(id,10, i1, 4321 ,t1,"this too shall pass");
    index(id,11, i1, -987 ,t1,"An eye for eye only ends up making the whole world blind.");
    index(id,12, i1, 379 ,t1,"Great works are performed, not by strength, but by perseverance.");
    index(id,13, i1, 232 ,t1,"no eggs on wall, lesson learned", oddField, "odd man out");

    index(id, 14, "SubjectTerms_mfacet", new String[]  {"mathematical models", "mathematical analysis"});
    index(id, 15, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    index(id, 16, "SubjectTerms_mfacet", new String[]  {"test 1", "test 2", "test3"});
    String[] vals = new String[100];
    for (int i=0; i<100; i++) {
      vals[i] = "test " + i;
    }
    index(id, 17, "SubjectTerms_mfacet", vals);
    commit();

    handle.clear();
    handle.put("QTime", SKIPVAL);
    handle.put("timestamp", SKIPVAL);

    // these queries should be exactly ordered and scores should exactly match
    query("q","*:*", "sort",i1+" desc");
    query("q","*:*", "sort",i1+" desc", "fl","*,score");
    handle.put("maxScore", SKIPVAL);
    query("q","{!func}"+i1);// does not expect maxScore. So if it comes ,ignore it. JavaBinCodec.writeSolrDocumentList()
    //is agnostic of request params.
    handle.remove("maxScore");
    query("q","{!func}"+i1, "fl","*,score");  // even scores should match exactly here

    handle.put("highlighting", UNORDERED);
    handle.put("response", UNORDERED);

    handle.put("maxScore", SKIPVAL);
    query("q","quick");
    query("q","all","fl","id","start","0");
    query("q","all","fl","foofoofoo","start","0");  // no fields in returned docs
    query("q","all","fl","id","start","100");

    handle.put("score", SKIPVAL);
    query("q","quick","fl","*,score");
    query("q","all","fl","*,score","start","1");
    query("q","all","fl","*,score","start","100");

    query("q","now their fox sat had put","fl","*,score",
            "hl","true","hl.fl",t1);

    query("q","now their fox sat had put","fl","foofoofoo",
            "hl","true","hl.fl",t1);


    handle.put("debug", UNORDERED);
    handle.put("time", SKIPVAL);

    query("q","now their fox sat had put","fl","*,score",
            "debugQuery", "true");

    // TODO: This test currently fails because debug info is obtained only
    // on shards with matches.
    /***
    query("q","matchesnothing","fl","*,score",
            "debugQuery", "true");    
    ***/
    query("q","matchesnothing","fl","*,score");  


    query("q","*:*", "rows",100, "facet","true", "facet.field",t1);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","count");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.limit",-1, "facet.sort","index");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1,"facet.limit",1);
    query("q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*");
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.offset",1);
    query("q","*:*", "rows",100, "facet","true", "facet.field",t1, "facet.mincount",2);

    // test faceting multiple things at once
    query("q","*:*", "rows",100, "facet","true", "facet.query","quick", "facet.query","all", "facet.query","*:*"
    ,"facet.field",t1);

    // test filter tagging, facet exclusion, and naming (multi-select facet support)
    query("q","*:*", "rows",100, "facet","true", "facet.query","{!key=myquick}quick", "facet.query","{!key=myall ex=a}all", "facet.query","*:*"
    ,"facet.field","{!key=mykey ex=a}"+t1
    ,"facet.field","{!key=other ex=b}"+t1
    ,"facet.field","{!key=again ex=a,b}"+t1
    ,"facet.field",t1
    ,"fq","{!tag=a}id:[1 TO 7]", "fq","{!tag=b}id:[3 TO 9]"
    );
    query("q", "*:*", "facet", "true", "facet.field", "{!ex=t1}SubjectTerms_mfacet", "fq", "{!tag=t1}SubjectTerms_mfacet:(test 1)", "facet.limit", 10", "facet.mincount", "1");
    // test field that is valid in schema but missing in all shards
   query("q","*:*", "rows",100, "facet","true", "facet.field",missingField, "facet.mincount",2);
   // test field that is valid in schema and missing in some shards
    query("q","*:*", "rows",100, "facet","true", "facet.field",oddField, "facet.mincount",2);

    try {
      // test error produced for field that is invalid for schema
      query("q","*:*", "rows",100, "facet","true", "facet.field",invalidField, "facet.mincount",2);
      fail("SolrServerException expected for invalid field that is not in schema");
    } catch (SolrServerException ex) {
      // expected
    }

    // Thread.sleep(10000000000L);

    destroyServers();
  }

  Shard[] buildShards(int numShards, SolrConfig solrConf) throws IOException {
    IndexUpdateConfiguration iconf = new IndexUpdateConfiguration(conf);
    // no need to set analyzer class
    iconf.setIndexInputFormatClass(SolrXMLDocInputFormat.class);
    iconf.setLocalAnalysisClass(SolrLocalAnalysis.class);

    SolrIndexConfig solrIndexConf = solrConf.mainIndexConfig;
    if (solrIndexConf.maxFieldLength != -1)
      iconf.setIndexMaxFieldLength(solrIndexConf.maxFieldLength);
    iconf.setIndexUseCompoundFile(solrIndexConf.useCompoundFile);

    iconf.setIndexMaxNumSegments(1);

    long versionNumber = -1;
    long generation = -1;

    if (fs.exists(outputPath)) {
      fs.delete(outputPath, true);
    }

    if (fs.exists(indexPath)) {
      fs.delete(indexPath, true);
    }

    Shard[] shards = new Shard[numShards];
    for (int j = 0; j < shards.length; j++) {
      shards[j] =
          new Shard(versionNumber,
              new Path(indexPath, NUMBER_FORMAT.format(j)).toString(),
              generation);
    }
    build(shards);
    return shards;
  }

  void build(Shard[] shards) throws IOException {
    IndexUpdater updater = new SolrIndexUpdater();
    updater.run(conf, new Path[] { inputPath }, outputPath, numMapTasks,
        shards);

    // verify the done files
    Path[] doneFileNames = new Path[shards.length];
    int count = 0;
    FileStatus[] fileStatus = fs.listStatus(outputPath);
    for (int i = 0; i < fileStatus.length; i++) {
      FileStatus[] doneFiles = fs.listStatus(fileStatus[i].getPath());
      for (int j = 0; j < doneFiles.length; j++) {
        doneFileNames[count++] = doneFiles[j].getPath();
      }
    }
    assertEquals(shards.length, count);
    for (int i = 0; i < count; i++) {
      assertTrue(doneFileNames[i].getName().startsWith("done"));
      // IndexUpdateReducer.DONE.toString()));
    }

    // verify the index
    IndexReader[] readers = new IndexReader[shards.length];
    for (int i = 0; i < shards.length; i++) {
      Directory dir =
          new FileSystemDirectory(fs, new Path(shards[i].getDirectory()),
              false, conf);
      readers[i] = IndexReader.open(dir);
    }

    IndexReader reader = new MultiReader(readers);
    int numDocs = reader.numDocs();
    assertEquals(17, numDocs);

    reader.close();
  }

  void copyShardFiles(Shard[] shards) throws IOException {
    // copy into testDir/shardn/index
    FilenameFilter indexFileFilter = IndexFileNameFilter.getFilter();
    for (int i = 0; i < shards.length; i++) {
      PathFilter filter = new PathFilter() {
        public boolean accept(Path path) {
          return true;
        }
      };
      FileStatus[] files =
          fs.listStatus(new Path(shards[i].getDirectory()), filter);
      String dirStr =
          testDir.toString() + File.separator + "shard" + (i + 1)
              + File.separator + "index";
      File dir = new File(dirStr);
      dir.mkdirs();
      for (int j = 0; j < files.length; j++) {
        String name = files[j].getPath().getName();
        if (indexFileFilter.accept(null, name)) {
          fs.copyToLocalFile(files[j].getPath(), new Path(dirStr, name));
        }
      }
    }
  }

  void createXMLdoc(FileSystem fs, Path inputDir, int docName,
      Object... fields) throws IOException {
    FSDataOutputStream out = fs.create(new Path(inputDir, docName + ".xml"));
    try {
      String[] fieldsInString = convertFields(fields);
      String docString = adoc(fieldsInString);
      out.writeBytes(docString);
    } finally {
      if (out != null) {
        out.close();
      }
    }
  }
  
  String[] convertFields(Object... fields) {
    ArrayList<String> fieldArray = new ArrayList<String>();
    for (int i = 0; i < fields.length; i += 2) {
      String name = (String) fields[i];
      Object value = fields[i + 1];
      if (value instanceof Object[]) {
        Object[] objects = (Object[]) value;
        for (int j = 0; j < objects.length; j++) {
          fieldArray.add(name);
          fieldArray.add(objects[j].toString());
        }
      } else {
        fieldArray.add(name);
        fieldArray.add(value.toString());
      }
    }
    return fieldArray.toArray(new String[fieldArray.size()]);
  }

  // wish could use AbstractSolrTestCase, but that creates TestHarness
  public String adoc(String... fieldsAndValues) {
    Doc d = doc(fieldsAndValues);
    return add(d);
  }

  public String add(Doc doc, String... args) {
    try {
      StringWriter r = new StringWriter();

      if (null == args || 0 == args.length) {
        r.write("<add>");
        r.write(doc.xml);
        r.write("</add>");
      } else {
        XML.writeUnescapedXML(r, "add", doc.xml, (Object[]) args);
      }

      return r.getBuffer().toString();
    } catch (IOException e) {
      throw new RuntimeException(
          "this should never happen with a StringWriter", e);
    }
  }

  public Doc doc(String... fieldsAndValues) {
    Doc d = new Doc();
    d.xml = makeSimpleDoc(fieldsAndValues).toString();
    return d;
  }

  public static StringBuffer makeSimpleDoc(String[] fieldsAndValues) {

    try {
      StringWriter w = new StringWriter();
      w.append("<doc>");
      for (int i = 0; i < fieldsAndValues.length; i += 2) {
        XML.writeXML(w, "field", fieldsAndValues[i + 1], "name",
            fieldsAndValues[i]);
      }
      w.append("</doc>");
      return w.getBuffer();
    } catch (IOException e) {
      throw new RuntimeException(
          "this should never happen with a StringWriter", e);
    }
  }

}