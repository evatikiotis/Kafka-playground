package producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class SimpleKafkaProducer {
    public static void main(String[] args) throws IOException {

        String bootstrapServers = "localhost:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        String topic = "test";
        ProducerRecord<String, String> record = new ProducerRecord(topic, "key", "I come from SimpleKafkaProducer");
        // Split to get key
        producer.send(record);
//        flush and close
        producer.close();
    }
}
