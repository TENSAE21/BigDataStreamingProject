package com.cs523.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

public class DataProducer {
	
	private static BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
	private static Client hosebirdClient;
	
	private static Producer<String, String> producer;
	
	public static void main(String[] args) throws InterruptedException {
		
		DataProducer dataProducer = new DataProducer();
		
		producer = dataProducer.createProducer();
		
		dataProducer.consumeLiveStreamAndPublishToQueue(producer);
		
		Runtime.getRuntime().addShutdownHook(shutDownHookListener);
	}
	
	private void consumeLiveStreamAndPublishToQueue(Producer<String, String> kafkaProducer) throws InterruptedException{
		
		hosebirdClient = createHoseBirdClient();
		
		// Connect Client
		hosebirdClient.connect();
		
		long i = 0;
		while (!hosebirdClient.isDone()) {
			
     		  String msg = msgQueue.take();
     		  
     		  System.out.println("Writing data --- ");
     		  i++;
     		  kafkaProducer.send(new ProducerRecord<String, String>(AppConfig.Kafka.TOPIC, String.valueOf(i), msg));
			
		}
	}
	
	private static Client createHoseBirdClient(){
		
		Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
		StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
		
		List<String> terms = Lists.newArrayList("vaccination");
		hosebirdEndpoint.trackTerms(terms);

		Authentication hosebirdAuth = new OAuth1(AppConfig.Twitter.CUSTOMER_API_KEY, AppConfig.Twitter.CUSTOMER_API_SECRET, AppConfig.Twitter.AUTH_TOKEN, AppConfig.Twitter.AUTH_TOKEN_SECRET);

		
		ClientBuilder builder = new ClientBuilder()
		.name("Inge")
		.hosts(hosebirdHosts)
		.authentication(hosebirdAuth)
		.endpoint(hosebirdEndpoint)
		.processor(new StringDelimitedProcessor(msgQueue));
		
		return builder.build();
	}
	
	private Producer<String, String> createProducer(){
		
		Properties props = new Properties();
		
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.Kafka.KAFKA_BROKERS);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AppConfig.Kafka.KEY_SERIALIZER_CLASS);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AppConfig.Kafka.VALUE_SERIALIZER_CALSS);
		props.put(ProducerConfig.ACKS_CONFIG, AppConfig.Kafka.ACKNOWLEDGEMENT_ALL);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		 
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		return producer;
	}
	
	private static Thread shutDownHookListener = new Thread(){
		
		@Override
		public void run() {
			
			hosebirdClient.stop();
			producer.close();
		};
		
	};

}
