package consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Pattern;

public class SimpleKafkaConsumer {
    public static void main(String[] args) throws Exception {

        // Setting the time characteristics for the windows implementation
        String bootsrtapServers = "localhost:9092";
        String topic = "test";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrtapServers);
        // Add the kafka topic declared above as data source.*/
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-myapp");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        Pattern topicPattern = Pattern.compile(topic);
        consumer.subscribe(topicPattern);
        ConsumerRecords<String, String> records = consumer.poll( Duration.ofSeconds(10));
        records.forEach((each) -> System.out.println(each.key() + " " + each.value()));


    }
}
