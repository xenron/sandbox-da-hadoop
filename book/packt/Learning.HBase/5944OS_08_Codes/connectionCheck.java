import java.io.IOException;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;

public class connectionCheck throws MasterNotRunningException, ZooKeeperConnectionException {

    {
        public static void main(String[] args) throws IOException {

            try {
                HBaseAdmin.checkHBaseAvailable(conf);
            } catch (Exception e) {
                System.err.println("Exception at " + e);
                System.exit(1);
            }
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "infinity");
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            HTable table = new HTable(conf, "table");
            HBaseAdmin admin = new HBaseAdmin(conf);

            try {
                HBaseAdmin.checkHBaseAvailable(conf);
		System.out.println("connection made ! ");
            } catch (Exception error) {
                System.err.println("Error connecting HBase:  " + error);
                System.exit(1);
            }
        }
    }
