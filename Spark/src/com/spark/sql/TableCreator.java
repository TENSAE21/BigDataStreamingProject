package com.spark.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class TableCreator {

	private static String driverName = "org.apache.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws ClassNotFoundException,
			SQLException {

		Class.forName(driverName);

		Connection con = DriverManager.getConnection("jdbc:hive2://localhost:10000/default", "cloudera", "cloudera");
		Statement stmt = con.createStatement();

		stmt.execute("DROP TABLE tweets");
		stmt.execute("CREATE EXTERNAL TABLE tweets(created_at string, text string, name string, "
				+ "followers_count int, friends_count int, retweet_count int, reply_count int) "
				+ "ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe' WITH SERDEPROPERTIES ( \"ignore.malformed.json\" = \"true\")"
				+ "LOCATION 'hdfs://quickstart.cloudera:8020/cloudera/home/tweets/'");
		
		System.out.println("Table tweets created ---");
		
		
		
		con.close();

	}
}
