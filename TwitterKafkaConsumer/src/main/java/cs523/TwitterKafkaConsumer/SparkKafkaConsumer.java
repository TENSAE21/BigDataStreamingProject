package main.java.cs523.TwitterKafkaConsumer;


import java.net.URI;
import java.util.*;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class SparkKafkaConsumer {
	
	static String SAVE_LOCATION = "hdfs://localhost:8020/user/cloudera/TwitterOutput";
	
	public static void main(String[] args) throws Exception
	{
		
		String hdfsuri = "hdfs://localhost:8020"; //54310
		
		Configuration conf = new Configuration();
	      // Set FileSystem URI
	      conf.set("fs.defaultFS", hdfsuri);
	      // Because of Maven
	      conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
	      conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
	      // Set HADOOP user
	      System.setProperty("HADOOP_USER_NAME", "cloudera");
	      System.setProperty("HADOOP_PASSWORD", "cloudera");
	      System.setProperty("hadoop.home.dir", "/");
	      //Get the filesystem - HDFS
	      FileSystem fs = FileSystem.get(URI.create(hdfsuri), conf);

	      //==== Create folder if not exists
	      Path workingDir=fs.getWorkingDirectory();
	      Path newFolderPath= new Path(SAVE_LOCATION);
	      if(!fs.exists(newFolderPath)) {
	         // Create new Directory
	         fs.mkdirs(newFolderPath);
	         System.out.println("Path "+SAVE_LOCATION+" created.");
	      }

	      //==== Write file
	      System.out.println("Begin Write file into hdfs");
//	      //Create a path
//	      Path hdfswritepath = new Path(newFolderPath + "/" + fileName);
//	      //Init output stream
//	      FSDataOutputStream outputStream=fs.create(hdfswritepath);
//	      //Cassical output stream usage
//	      outputStream.writeBytes(fileContent);
//	      outputStream.close();
//	      logger.info("End Write file into hdfs");

		// Create a Java Spark Context
		SparkConf sparkConf = new SparkConf();
		sparkConf.setAppName("SparkKafka");
		sparkConf.setMaster("local");

		JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		
	
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put("bootstrap.servers", "localhost:9092");
		kafkaParams.put("key.deserializer", StringDeserializer.class.getName());
		kafkaParams.put("value.deserializer", StringDeserializer.class.getName());
		kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
		kafkaParams.put("auto.offset.reset", "latest");
		kafkaParams.put("enable.auto.commit", false);
		
		Collection<String> topics = Arrays.asList("sessiondata");

		JavaInputDStream<ConsumerRecord<String, String>> stream =
				  KafkaUtils.createDirectStream(
				    streamingContext,
				    LocationStrategies.PreferConsistent(),
				    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
				  );
		
		
		JavaDStream<String> dstream = stream.map(st -> convertTweetToJSON(st.value()))
		.filter(tweet -> tweet != null)
		.filter(tweet -> !tweet.isEmpty());
		
	
		dstream.foreachRDD(rdd -> {
			
			if(!rdd.isEmpty()){
				rdd.saveAsTextFile(SAVE_LOCATION);
				System.out.println("saving rdd ...###" );
				
		}
		});
				
		streamingContext.start();
		streamingContext.awaitTermination();

	}
	
	
	private static String convertTweetToJSON(String tweet){
        try{
        	
            JsonObject tweetObject = new JsonParser().parse(tweet).getAsJsonObject();
           
            String createdAt = tweetObject.get("created_at").getAsString();
            String text = tweetObject.get("text").getAsString();
            JsonObject userObject = tweetObject.get("user").getAsJsonObject();
            String name = userObject.get("name").getAsString();
           
            int followersCount = userObject.get("followers_count").getAsInt();
            int friendsCount = userObject.get("friends_count").getAsInt();
            int retweetCount = tweetObject.get("retweet_count").getAsInt();
            int replyCount = tweetObject.get("reply_count").getAsInt();
            
            
            JsonObject object = new JsonObject();
            object.addProperty("created_at", createdAt);
            object.addProperty("text", text);
            object.addProperty("name", name);
            object.addProperty("followers_count", followersCount);
            object.addProperty("friends_count", friendsCount);
            object.addProperty("retweet_count", retweetCount);
            object.addProperty("reply_count", replyCount);
            return object.toString();
        }catch(Exception e){
            return null;
        }
    }

}
