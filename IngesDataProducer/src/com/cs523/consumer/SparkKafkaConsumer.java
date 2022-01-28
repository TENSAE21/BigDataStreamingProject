package com.cs523.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.json.JSONObject;

import com.vdurmont.emoji.EmojiParser;

public class SparkKafkaConsumer {

	private static JavaStreamingContext streamingContext;

	static Configuration conf = new Configuration();

	public static final String hdfs = "hdfs://localhost:8020";

	public static final String hpath = "/cloudera/home/tweets/";
	
	public static final String fileName = "tweet";
	
	private static FSDataOutputStream out = null;
	
	private static Pattern pattern = Pattern.compile("[a-zA-Z0-9]+");

	public static void main(String[] args) throws InterruptedException {

		conf.set("fs.defaultFS", hdfs);
		conf.set("hadoop.job.ugi", "hdfs");
		conf.set("dfs.replication", "1");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "false");

		SparkConf conf = new SparkConf().setMaster("local").setAppName(
				"IngeSparkKafkaConsumer");

		streamingContext = new JavaStreamingContext(conf, Durations.seconds(5));

		streamingContext.sparkContext().setLogLevel("WARN");

		JavaInputDStream<ConsumerRecord<String, String>> inputDStream = createInputDStream(streamingContext);

		consumeAndStoreStreams(inputDStream);

		Runtime.getRuntime().addShutdownHook(shutDownHookListener);

		streamingContext.start();
		streamingContext.awaitTermination();

	}

	private static void consumeAndStoreStreams(
			JavaInputDStream<ConsumerRecord<String, String>> inputDStream) {

		JavaDStream<String> dstream = inputDStream
				.map(stream -> convertTweetToJSON(stream.value()))
				.filter(tweet -> tweet != null)
				.filter(tweet -> !tweet.isEmpty());

		dstream.foreachRDD((rdd, time) -> {

			if (!rdd.isEmpty()) {

				
				try {
									
					FileSystem fs = FileSystem.get(conf);
					Path filenamePath = new Path(hpath + fileName);
					
					if(!fs.exists(filenamePath)){
						
						out = fs.create(filenamePath);
					}else{
						out = fs.append(filenamePath);
					}
					
					rdd.collect().forEach(string -> {
						
						System.out.println("Data received ---" + string);
						try {
							
							out.writeBytes(string + "\r\n");
						} catch (Exception e) {
							e.printStackTrace();
						}
						
					});
					
					
					System.out.println("written to file");
					out.close();
					
				} catch (Exception e) {
					
					e.printStackTrace();
				}
				
			}
		});
	}

	private static JavaInputDStream<ConsumerRecord<String, String>> createInputDStream(
			JavaStreamingContext streamingContext) {

		Map<String, Object> kafkaParams = new HashMap<String, Object>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				AppConfig.Kafka.KAFKA_BROKERS);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				AppConfig.Kafka.KEY_DESERIALIZER_CLASS);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				AppConfig.Kafka.VALUE_DESERIALIZER_CALSS);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG,
				AppConfig.Kafka.GROUP_ID);
		kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
				AppConfig.Kafka.AUTO_OFFSET_RESET);
		kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

		Collection<String> topics = Arrays.asList(AppConfig.Kafka.TOPIC);

		JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils
				.createDirectStream(streamingContext, LocationStrategies
						.PreferConsistent(), ConsumerStrategies
						.<String, String> Subscribe(topics, kafkaParams));

		return stream;
	}

	private static String convertTweetToJSON(String tweet) {

		try {

			JSONObject tweetObject = new JSONObject(tweet);;

			String createdAt = tweetObject.getString("created_at");
			String text = tweetObject.getString("text");

			JSONObject userObject = tweetObject.getJSONObject("user");

			String name = userObject.getString("name");
			int followersCount = userObject.getInt("followers_count");
			int friendsCount = userObject.getInt("friends_count");

			int retweetCount = tweetObject.getInt("retweet_count");
			int replyCount = tweetObject.getInt("reply_count");

			
			JSONObject object = new JSONObject();
			object.put("created_at", createdAt);
			object.put("text", EmojiParser.removeAllEmojis(text));
			object.put("name", EmojiParser.removeAllEmojis(name));
			object.put("followers_count", followersCount);
			object.put("friends_count", friendsCount);
			object.put("retweet_count", retweetCount);
			object.put("reply_count", replyCount);

			return object.toString();

		} catch (Exception e) {

			return null;
		}
	}

	private static Thread shutDownHookListener = new Thread() {

		@Override
		public void run() {

			streamingContext.close();
		};
	};
	
	

}
