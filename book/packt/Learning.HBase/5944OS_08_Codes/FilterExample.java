import java.io.IOException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes.toBytes;

public class FilterExample {
        public static void main(String[] arguments) throws IOException {
                Configuration config = HBaseConfiguration.create();
                HTable hbaseTableObj = new HTable(config, "logTable");
                Scan scanObj = new Scan();
                scanObj.setFilter(new ValueFilter(CompareOp.EQUAL, new SubstringComparator("shash")));
                ResultScanner resultScannerObj = hbaseTableObj.getScanner(scanObj);                
                for ( Result result : resultScannerObj){
                        byte [] value = result.getValue(toBytes("ColFamily"), toBytes("columnName"));
                        System.out.println(Bytes.toString(value));
                } 
                resultScannerObj.close();
                hbaseTableObj.close();
        }
}