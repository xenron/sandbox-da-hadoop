import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
public class scanInBatch {
 public static void main(String[] args) throws IOException {
                Configuration config = HBaseConfiguration.create();
                HTable tableToScanObj = new HTable(config, "logTable");
                Scan scanObj = new Scan();
                scan.addFamily(toBytes("columns"));
                scanDisplayData(tableToScanObj, scanObj); 
                scan.setBatch(2);
                scanDisplayData(tableToScanObj, scanObj); 
                tableToScanObj.close();
        }
        private static void scanDisplayData(HTable tableToScanObj, Scan scanObj) throws IOException {
                System.out.println("Batch Number : " + scanObj.getBatch());
                ResultScanner resultScannerObj = tableToScanObj.getScanner(scanObj);
                for ( Result result : resultScannerObj){
                        System.out.println("Data : ");
                        for ( KeyValue keyValuePairObj : result.list()){
                                System.out.println(Bytes.toString(keyValuePairObj.getValue()));
                        }
                }
                resultScannerObj.close();
        }
}