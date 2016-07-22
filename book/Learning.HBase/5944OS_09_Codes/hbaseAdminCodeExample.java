import java.io.IOException;
import java.util.Collection;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HServerInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
public class hbaseAdminCodeExample {
		private static Configuration conf = null;
		HBaseAdmin admin = null;
		public hbaseAdminCodeExample(){
 
    }
 public static void main(String[] args) throws IOException {
        for (int i = 0; i < args.length; i++) {
            System.out.println("Argument Specified" + i + ":" + args[i]);
        }
		config.set("hbase.zookeeper.quorum", "infinity");
		config.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseAdminCodeExample adminObj = new hbaseAdminCodeExample();
		adminObj.printClusterDetails();//this will print hbase cluster details.
		//rest of the methods also can be called as adminObj.<method name with arguments>
    }
		
     static {
        config = HBaseConfiguration.create();
		admin=new HBaseAdmin(config);
    }
    public void addColumnToTable(String tableObj, String columnObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.addColumnToTable(tableObj, new HColumnDescriptor(columnObj));
        System.out.println("Added column : " + columnObj + "to table " + tableObj);
    }
    public void delColumnFromTable(String tableObj, String columnObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.deleteColumn(tableObj, columnObj);
        System.out.println("Deleted column : " + columnObj + "from table " + tableObj);
    }
    public void createTableInHbase(String tableObj, String ColFamName) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        HTableDescriptor tabledescriptor = new HTableDescriptor(Bytes.toBytes(tableObj));
        tabledescriptor.addFamily(new HColumnDescriptor(ColFamName));
        adminObj.createTableInHbase(tabledescriptor);
    }
    public void performMajorCompact(String mytable) throws IOException {
        config = HBaseConfiguration.create();
        HTable table = new HTable(config, mytable);
        HBaseAdmin adminObj = new HBaseAdmin(config);
        String tableObj = table.toString();
        try {
            adminObj.majorCompact(tableObj);
            System.out.println("Compaction done!");
        } catch (Exception e) {
            System.out.println(e);
        }
    }
    public static void checkIfRunningFine() throws MasterNotRunningException, ZooKeeperConnectionException {
        config = HBaseConfiguration.create();
        try {
            HBaseAdmin.checkHBaseAvailable(config);	
        } catch (Exception e) {
            System.err.println("Exception at " + e);
            System.exit(1);
        }
    }
    public void perfomrMinorcompact(String tabName) throws IOException, InterruptedException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.compact(tabName);
    }
    public void deletetableFromHBase(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.deleteTable(tableObj);
    }
    public void disableHBaseTable(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.disableTable(tableObj);
    }
    public void enableHBaseTable(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.enableTable(tableObj);
    }
    public void flushTable(String tabName) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.disableTable(tabName);
    }
    public ClusterStatus getHBaseclusterstatus() throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        return adminObj.getClusterStatus();
    }
    public void printClusterDetails() throws IOException {
        ClusterStatus clusterStatus = getclusterstatus();
        clusterStatus.getServerInfo();
        Collection < HServerInfo > serverinfo = clusterStatus.getServerInfo();
        for (HServerInfo s: serverinfo) {
            System.out.println("Server name " + s.getServerName());
            System.out.println("Host name " + s.getHostname());
            System.out.println("Host name : Port " + s.getHostnamePort());
            System.out.println("Info port" + s.getInfoPort());
            System.out.println("Server load " + s.getLoad().toString());
            System.out.println();
        }
        String version = clusterStatus.getHBaseVersion();
        System.out.println("Version " + version);
        int regioncounts = clusterStatus.getRegionsCount();
        System.out.println("Region Counts :" + regioncounts);
        int servers = clusterStatus.getServers();
        System.out.println("Servers :" + servers);
        double averageload = clusterStatus.getAverageLoad();
        System.out.println("Average load: " + averageload);
        int deadservers = clusterStatus.getDeadServers();
        System.out.println("Deadservers : " + deadservers);
        Collection < String > Servernames = clusterStatus.getDeadServerNames();
        for (String s: Servernames) {
            System.out.println("Dead Servernames " + s);
        }
    }
    public void isHBaseTableAvailable(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        boolean result = adminObj.isTableAvailable(tableObj);
        System.out.println("Table " + tableObj + " available ?" + result);
    }
    public void isHBaseTableEnabled(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        boolean result = adminObj.isTableEnabled(tableObj);
        System.out.println("Table " + tableObj + " enabled ?" + result);
    }
    public void isHBaseTableDisabled(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        boolean result = adminObj.isTableDisabled(tableObj);
        System.out.println("Table " + tableObj + " disabled ?" + result);
    }
    public void checkIfTableExists(String tableObj) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        boolean result = adminObj.tableExists(tableObj);
        System.out.println("Table " + tableObj + " exists ?" + result);
    }
    public void shutdownCluster() throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        System.out.println("Shutting down..");
        adminObj.shutdown();
    }
    public void listAllTablesInHBase() throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.listTables();
    }
    public void modifyTableColumn(String tableObj, String columnname, String descriptor) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.modifyColumn(tableObj, columnname, new HColumnDescriptor(descriptor));
    }
    public void modifyHBaseTable(String tableObj, String hbaseNewTableName) throws IOException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.modifyTable(Bytes.toBytes(tableObj), new HTableDescriptor(hbaseNewTableName));
    }
    public void splitHBaseTable(String tableObj) throws IOException, InterruptedException {
        config = HBaseConfiguration.create();
        HBaseAdmin adminObj = new HBaseAdmin(config);
        adminObj.split(tableObj);
    }
    public void checkIfMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
        config = HBaseConfiguration.create();
        HBaseAdmin administer = new HBaseAdmin(config);
        System.out.println("Master running ? " + administer.isMasterRunning());
    }
}