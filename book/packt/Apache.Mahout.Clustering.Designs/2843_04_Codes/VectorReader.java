import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.VectorWritable;

public class VectorReader {

	public void readFiles() throws Exception{
		    FileSystem fs = null;
		    fs = FileSystem.get(getConfiguration());
			SequenceFile.Reader reader = new SequenceFile.Reader(fs,new Path("<Input Path>"), getConfiguration());
			Text key = new Text();
			VectorWritable value = new VectorWritable();
			while (reader.next(key, value)) {
				System.out.println(key.toString() + " "	+ value.get().asFormatString());
			}
			reader.close();
	}
			
			private Configuration getConfiguration(){
				Configuration conf = new Configuration();
				conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
				conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
				return conf;
			}

}
