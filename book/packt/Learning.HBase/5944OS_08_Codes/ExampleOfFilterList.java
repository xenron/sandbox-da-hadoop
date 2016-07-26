import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import java.io.IOException;
import org.apache.hadoop.hbase.util.Bytes.toBytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.util.Bytes;
public class ExampleOfFilterList {
        public static void main(String[] arguments) throws IOException {
                Configuration config = HBaseConfiguration.create();
                HTable hbaseTableObj = new HTable(config, "logTable");
                Scan scanObj = new Scan();
                FilterList filterListObj = new FilterList(Operator.MUST_PASS_ALL);
                filterListObj.addFilter(new KeyOnlyFilter());
                filterListObj.addFilter(new FirstKeyOnlyFilter());
                scanObj.setFilter(filterListObj);
                ResultScanner resultScannerObj = hbaseTableObj.getScanner(scanObj);                
                for ( Result result : resultScannerObj){
                        byte [] value = result.getValue(toBytes("colFamName"), toBytes("colName"));
                        System.out.println("Value found :" +Bytes.toString(value));
                } 
                resultScannerObj.close();
                hbaseTableObj.close();
        }
}