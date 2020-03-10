package kafka_producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class KafkaMain {
    public static void main(String[] args) throws IOException {

//        final Logger logger = LoggerFactory.getLogger(KafkaProducerToFlink.class);
        String bootstrapServers = "127.0.0.1:9092";
//        create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        String topic = "test_topic_source_5";

        String csvFile = "openaqDataSet.csv";
        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        br = new BufferedReader(new FileReader(csvFile));
        // Split to get key
        while ((line = br.readLine()) != null) {
            String[] words = line.split(cvsSplitBy);
            //        send data - asynchronous
            String key = words[2];
            ProducerRecord<String, String> record = new ProducerRecord(topic, key, line);
            producer.send(record);
        }

//        flush and close
        producer.close();
    }

}
