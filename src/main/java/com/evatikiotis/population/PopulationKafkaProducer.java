package com.evatikiotis.population;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.util.Properties;

public class PopulationKafkaProducer {
    public static void main(String[] args) throws IOException {
        final String DATASET_FILENAME_AND_EXT = "small-population-dataset.csv";
        final String BOOTSTRAP_SERVERS = "localhost:9092";
        final String TOPIC = "custom-population";
        final String CSV_SPLIT_CHARACTER = ",";

        // define Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create BufferedReader to read file line by line and produce records
        BufferedReader br = new BufferedReader(new FileReader(DATASET_FILENAME_AND_EXT));
        // read first line and ignore it
        String line = br.readLine();
        if (line == null) {
            throw new FileSystemException(DATASET_FILENAME_AND_EXT);
        }

        while ((line = br.readLine()) != null) {
            // remove '.' from our line
            line = String.join("", line.split("\\."));
            String[] words = line.split(CSV_SPLIT_CHARACTER);
            if (words.length != 2) {
                continue;
            }
            String key = words[0];
            // produce record in kafka topic and send it
            ProducerRecord<String, String> record = new ProducerRecord(TOPIC, key, line);
            producer.send(record);
        }

//        flush and close
        producer.close();
    }

}
