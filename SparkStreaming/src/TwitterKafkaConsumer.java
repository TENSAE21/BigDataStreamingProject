import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;


import java.util.Collections;
import java.util.Properties;

public class TwitterKafkaConsumer {
	private final static String TOPIC = "sessiondata";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	
    public static void main(String... args) throws Exception {
        runConsumer();
    }
    
    private static Consumer<String, String> createConsumer() {
    	//	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfig.Kafka.KAFKA_BROKERS);
	//	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, AppConfig.Kafka.KEY_SERIALIZER_CLASS);
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AppConfig.Kafka.VALUE_SERIALIZER_CALSS);
		//props.put(ProducerConfig.ACKS_CONFIG, AppConfig.Kafka.ACKNOWLEDGEMENT_ALL);
		//props.put(ProducerConfig.RETRIES_CONFIG, 0);
		 
		//Producer<String, String> producer = new KafkaProducer<String, String>(props);
    	
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                    BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG,
                                    "TwitterKafkaConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                                    new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }
    
    
    static void runConsumer() throws InterruptedException {
        final Consumer<String, String> consumer = createConsumer();

        final int giveUp = 100;   int noRecordsCount = 0;

        while (true) {
            final ConsumerRecords<String, String> consumerRecords =
                    consumer.poll(1000);

            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) break;
                else continue;
            }

            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });

            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
}
