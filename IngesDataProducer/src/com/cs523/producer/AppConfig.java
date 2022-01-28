package com.cs523.producer;

public class AppConfig {
	
	public static class Twitter{
		
		//Use twitter Developer account details for the following fields
		public static final String CUSTOMER_API_KEY = ""; //removed for security
		
		public static final String CUSTOMER_API_SECRET = ""; //removed for security
		
		
		public static final String AUTH_TOKEN = ""; //removed for security
		
		public static final String AUTH_TOKEN_SECRET = ""; //removed for security
	}
	
	public static class Kafka{
		
		public static final String TOPIC = "inge-events";
		
		public static final String KAFKA_BROKERS = "localhost:9092";
		 
		public static final String KEY_SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer";
		
		public static final String VALUE_SERIALIZER_CALSS = "org.apache.kafka.common.serialization.StringSerializer";
		
		public static final String ACKNOWLEDGEMENT_ALL = "all";
	}

}
