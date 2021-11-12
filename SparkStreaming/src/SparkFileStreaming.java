import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkFileStreaming {
	
	public static void main(String[] args){
		SparkSession spark = SparkSession.builder().appName("spark streaming").config("spark.master", "local").config("spark.sql.warehouse.dir", "file:///app/").getOrCreate();
		spark.sparkContext().setLogLevel("ERROR");

        

        //define schema type of file data source

        StructType schema = new StructType().add("empId", DataTypes.StringType).add("empName", DataTypes.StringType)

                .add("department", DataTypes.StringType);

        //build the streaming data reader from the file source, specifying csv file format  

        Dataset<Row> rawData = spark.readStream().option("header", true).format("csv").schema(schema).csv("/home/cloudera/Desktop/Project/streamingfiles/*.csv");
		
        rawData.createOrReplaceTempView("empData");

	      //count of employees grouping by department

	        Dataset<Row> result = spark.sql("select count(*), Department from  empData group by Department");
	        StreamingQuery query;
			try {
				query = result.writeStream().outputMode(OutputMode.Update()).format("console").start();
				query.awaitTermination();
			} catch (TimeoutException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (StreamingQueryException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		System.out.println("Hope to write to Hive sometime today");
		
	}

}
