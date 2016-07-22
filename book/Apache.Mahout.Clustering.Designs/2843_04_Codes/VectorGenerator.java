import java.io.File;
import java.io.FileReader;
import java.io.Reader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.NamedVector;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.utils.vectors.csv.CSVVectorIterator;

public class VectorGenerator {
	
	public  void returnVector() throws Exception{
		Path vecoutput = new Path("<OutputPath>");//output path
		FileSystem fs = FileSystem.get(getConfiguration());
		Reader r = new FileReader(new File("<InputPath>"));//input path
		SequenceFile.Writer writer;
		CSVVectorIterator ctr = new CSVVectorIterator(r);
		writer = new SequenceFile.Writer(fs, getConfiguration(), vecoutput, Text.class, VectorWritable.class);
		VectorWritable vec = new VectorWritable();
		while(ctr.hasNext()){
			NamedVector nmv = new NamedVector(ctr.next(),"Dummy");
            vec.set(nmv);
            writer.append(new Text( nmv.getName()), vec);	
		
		}
		writer.close();
	}
	
	private  Configuration getConfiguration(){
		Configuration conf = new Configuration();
		conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
		conf.addResource(new Path("/etc/hadoop/conf/yarn-site.xml"));
		return conf;
	}


}
