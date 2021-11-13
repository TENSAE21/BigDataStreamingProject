package main.java.cs523.TwitterKafkaConsumer;


import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


public class SparkKafkaConsumer {
	
	static String SAVE_LOCATION = "output";
	
	public static void main(String[] args) throws Exception
	{
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
            object.addProperty("followers_count", String.valueOf(followersCount));
            object.addProperty("friends_count", String.valueOf(friendsCount));
            object.addProperty("retweet_count", String.valueOf(retweetCount));
            object.addProperty("reply_count", String.valueOf(replyCount));
            return object.toString();
        }catch(Exception e){
            return null;
        }
    }

}
