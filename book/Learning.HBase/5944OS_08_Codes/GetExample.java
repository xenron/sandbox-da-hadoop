import static org.apache.hadoop.hbase.util.Bytes.*;
import java.io.IOException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
public class GetExample {
        public static void main(String[] args) throws IOException {
                Configuration config = HBaseConfiguration.create();
                HTable tableObj = new HTable(config, "logtable");
                Get getObject = new Get(toBytes("rowKey1"));             
                Result getResult = tableObj.getObject(getObject);
                print(getResult);
                getObject.addColumn(toBytes("colFam"), toBytes("col2"));
                getResult = tableObj.getObject(getObject);
                print(getResult);
                tableObj.close();
        }
        private static void print(Result getResult) {
                System.out.println("Row Key: " + Bytes.toString(getResult.getRow()));
                byte [] value1 = getResult.getValue(toBytes("colFam"), toBytes("column1"));
                System.out.println("colFam1:colum1="+Bytes.toString(value1));
                byte [] value2 = getResult.getValue(toBytes("colFam"), toBytes("column2"));
                System.out.println("colFam1:column2="+Bytes.toString(value2));
        }

}