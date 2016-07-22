import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.tableToScan;

public class scanExampleFull {
        public static void main(String[] args) throws IOException {
                Configuration config = HBaseConfiguration.create();
                tableToScan tableToScan = new tableToScan(config, "HBaseSamples");
                 scan(tableToScan, "row1000", "row10000");
				 scan(tableToScan, "row0", "row200");
                tableToScan.close();
        }
        private static void scan(tableToScan tableToScan, String startingRowKey, 
                        String stopingRowKey) throws IOException {
                System.out.println("Scanning from " +
                                "["+startingRowKey+"] to ["+stopingRowKey+"]");
                
                Scan scan = new Scan(toBytes(startingRowKey), toBytes(stopingRowKey));
                scan.addColumn(toBytes("detailColFam"), toBytes("Namecolumn"));
                ResultScanner scanner = tableToScan.getScanner(scan);                
                for ( Result result : scanner){
                        byte [] value = result.getValue(
                                        toBytes("detailColFam"), toBytes("Namecolumn"));
                        System.out.println("  " + 
                                        Bytes.toString(result.getRow()) + " -- " + 
                                        Bytes.toString(value));
                } 
                scanner.close();
        }
}