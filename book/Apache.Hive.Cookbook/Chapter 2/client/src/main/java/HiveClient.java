import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveClient {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) {
	      try {
	      Class.forName(driverName);
	    } catch (ClassNotFoundException e) {
	      e.printStackTrace();
	      System.exit(1);
	    }
	    //replace "root" here with the name of the user the queries should run as
	    Connection conn;
		try {
		conn = DriverManager.getConnection("jdbc:hive2://192.168.56.101:10000/default", "root", "");
		
	    Statement stmt = conn.createStatement();
	    String table_name = "testtable";
	    stmt.execute("drop table if exists " + table_name);
	    stmt.execute("create table " + table_name + " (id int, fname string,age int)");
	    
	    // 1. show tables
	    String sqlQuery = "show tables";
	    System.out.println("Running query: " + sqlQuery);
	    ResultSet rst = stmt.executeQuery(sqlQuery);
	    if (rst.next()) {
	      System.out.println(rst.getString(1));
	    }
	    
	    // 2. describe table
	    sqlQuery = "describe " + table_name;
	    System.out.println("Executing query: " + sqlQuery);
	    rst = stmt.executeQuery(sqlQuery);
	    while (rst.next()) {
	      System.out.println(rst.getString(1) + "\t" + rst.getString(2));
	    }
	 
	    // 3. load data into table
	     /** filepath is local to hive server
	     NOTE: /opt/sample_10000.txt is a '\t' separated file with ID and First Name values. */
	    String filepath = "/opt/sample_10000.txt";
	    sqlQuery = "load data local inpath '" + filepath + "' into table " + table_name;
	    System.out.println("Executing query: " + sqlQuery);
	    stmt.execute(sqlQuery);
	 
	    // 4. select * query
	    sqlQuery = "select * from " + table_name;
	    System.out.println("Executing query: " + sqlQuery);
	    rst = stmt.executeQuery(sqlQuery);
	    while (rst.next()) {
	      System.out.println(String.valueOf(rst.getInt(1)) + "\t" + rst.getString(2));
	    }
	 
	    // 5. regular hive query
	    sqlQuery = "select count(*) from " + table_name;
	    System.out.println("Running: " + sqlQuery);
	    rst = stmt.executeQuery(sqlQuery);
	    while (rst.next()) {
	      System.out.println(rst.getString(1));
	    }
	  
	} catch (SQLException e) {
		e.printStackTrace();
	}
	}


}
