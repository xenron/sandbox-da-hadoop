import java.io.IOException;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import static org.apache.hadoop.hbase.util.Bytes.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
public class ExampleofPutOperation {
        public static void main(String[] arguments) throws IOException {
                Configuration config = HBaseConfiguration.create();
                HTable toWriteDataInTable = new HTable(config, "logTable");
                Put putObj = new Put(toBytes("logdataKey1"));
                putObj.add(toBytes("colFamily"), toBytes("columnName1"), toBytes("internetexplorer"));
                putObj.add(toBytes("colFamily"), toBytes("columnName2"), toBytes("123456"));
                toWriteDataInTable.put(putObj);
                toWriteDataInTable.close();
        }
}
