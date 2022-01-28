package com.cs523.consumer;

public class AppConfig {

	public static class Kafka {

		public static final String TOPIC = "inge-events";

		public static final String KAFKA_BROKERS = "localhost:9092";

		public static final String KEY_DESERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringDeserializer";

		public static final String VALUE_DESERIALIZER_CALSS = "org.apache.kafka.common.serialization.StringDeserializer";

		public static final String ACKNOWLEDGEMENT_ALL = "all";
		
		public static final String AUTO_OFFSET_RESET = "latest";
		
		public static final String GROUP_ID = "inge_group";
		
	}
	
	public static class HDFS{
		
		public static final String PATH = "hdfs://quickstart.cloudera:8020/cloudera/home/tweets/";
	}

}
