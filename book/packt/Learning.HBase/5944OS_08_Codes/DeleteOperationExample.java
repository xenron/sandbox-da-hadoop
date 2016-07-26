import static org.apache.hadoop.hbase.util.Bytes.toBytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import java.io.IOException;
public class DeleteOperationExample {
        public static void main(String[] arguments) throws IOException {
                Configuration config = HBaseConfiguration.create();
                HTable tableToDeleteDataFrom = new HTable(config, "logTable");
                Delete deleteobj1 = new Delete(toBytes("rowIDToDelete"));             
                tableToDeleteDataFrom.delete(deleteobj1);
                Delete deleteobj2 = new Delete(toBytes("2ndRowIDToDelete"));
                delete1.deleteColumns(toBytes("columnFamily"), toBytes("columnName"));
                tableToDeleteDataFrom.delete(deleteobj2);
                tableToDeleteDataFrom.close();
        }}
