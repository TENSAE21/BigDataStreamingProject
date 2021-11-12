
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class HiveWriter {
	private static String driverName = "org.apache.hive.jdbc.HiveDriver";
	public static void main (String[] args){
		
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Connection con;
		try {
			con = DriverManager.getConnection("jdbc:hive2://localhost:10000/database_prog", "cloudera", "cloudera");
			Statement stmt = con.createStatement();
			//stmt.execute("CREATE DATABASE database_prog");
		    //System.out.println("Database userdb created successfully");
		    
		    
//		    stmt.execute("CREATE TABLE IF NOT EXISTS "
//		            +" employee ( eid int, name String, "
//		            +" salary String, designation String)"
//		            +" COMMENT 'Employee details'"
//		            +" ROW FORMAT DELIMITED"
//		            +" FIELDS TERMINATED BY '\t'"
//		            +" LINES TERMINATED BY '\n'"
//		            +" STORED AS TEXTFILE");
//		            
//		         System.out.println("Table employee created.");
			
			  stmt.execute("LOAD DATA LOCAL INPATH '/home/cloudera/Desktop/Project/sampleEmps.txt'" + "OVERWRITE INTO TABLE employee");
		      System.out.println("Load Data into employee successful");
		         con.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("An Exception");
			e.printStackTrace();
		}
	    
		
		
	}

}
