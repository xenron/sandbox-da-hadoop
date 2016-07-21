import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.IdentityMapper;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import junit.framework.TestCase;

public class IdentityMapperTest extends TestCase {
	
	private Mapper mapper;
	private MapDriver driver;
	
	@Before
	public void setUp(){
		mapper = new IdentityMapper();
		driver = new MapDriver(mapper);
	}
	
	@Test
	public void testIdentityMapper1(){
		driver.withInput(new Text("foo"), new Text("bar"))
			.withOutput(new Text("foo"), new Text("bar"))
			.runTest();
	}
	
	
	@Test
	public void testIdentityMapper2(){
		driver.withInput(new Text("foo"), new Text("bar"))
			.withOutput(new Text("foo2"), new Text("bar2"))
			.runTest();
	}
	
	

}
