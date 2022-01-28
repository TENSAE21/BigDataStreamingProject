package com.spark.sql;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkSQL {
	
	public static void main(String[] args){
		
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("Spark Sql Hive");
		sparkConf.setMaster("local");
		
	    sparkConf.set("hive.metastore.uris", "thrift://localhost:9083"); 
    	
	    SparkSession sparkSession = SparkSession.builder().appName("Spark SQL-Hive").config(sparkConf)
	    		.config("spark.sql.hive.hiveserver2.jdbc.url", "jdbc:hive2://localhost:10000")
	    		.config("hive.metastore.warehouse.external.dir", "/user/cloudera/tweets")
	    		.enableHiveSupport().getOrCreate();
	       
	    
        Dataset<Row>  tabledata = sparkSession.sql("SELECT * FROM tweets LIMIT 10");
        tabledata.show();
        
        tabledata = sparkSession.sql("SELECT * FROM tweets WHERE followers_count > 100 LIMIT 5");
        tabledata.show();
        
        tabledata = sparkSession.sql("SELECT * FROM tweets WHERE retweet_count > 50 LIMIT 5");
        tabledata.show();
        
        sparkSession.close();
	}

}



