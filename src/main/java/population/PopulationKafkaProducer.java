package population;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class PopulationKafkaProducer {
    public static void main(String[] args) throws IOException {
        final String DATASET_FILENAME_AND_EXT = "total-population-dataset.csv";
        final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
        final String TOPIC = "population";

        //        create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

//        create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        BufferedReader br = null;
        String line = "";
        String cvsSplitBy = ",";

        br = new BufferedReader(new FileReader(DATASET_FILENAME_AND_EXT));
        // Split to get key
        while ((line = br.readLine()) != null) {
            String[] words = line.split(cvsSplitBy);
            String key = words[2];
            ProducerRecord<String, String> record = new ProducerRecord(TOPIC, key, line);
            producer.send(record);
        }

//        flush and close
        producer.close();
    }

}
