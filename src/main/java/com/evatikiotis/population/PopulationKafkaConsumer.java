package com.evatikiotis.population;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 Group-by:Country,parameter and Calculate avg ,var, sd , si
 */
public class PopulationKafkaConsumer {

    public static void main(String[] args) throws Exception {
        final String TOPIC = "custom-population";
        final String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
        final long startTime = System.currentTimeMillis();


        // Create Streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(3);
        // Setting the time characteristics for the windows implementation
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // define consumer properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);

        // define the consumer
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(TOPIC, new SimpleStringSchema(), properties);
        flinkKafkaConsumer.setStartFromEarliest();

        // add consumer as environment datasource
        DataStream<String> kafkaData = env.addSource(flinkKafkaConsumer);
        DataStream<Tuple2<String, Long>> groupedByCountry = groupByCountry(kafkaData);

        // Create a sink for our stream.
        groupedByCountry.print();
        groupedByCountry.writeAsText("output_fistPass").setParallelism(1);
        // Execute job
        JobExecutionResult result = env.execute("My Flink Job");
        System.out.println("The job took " + result.getNetRuntime(TimeUnit.SECONDS) + " to execute");
        long endTime = System.currentTimeMillis();
        System.out.println("Total execution time: " + (endTime-startTime) + "ms");
    }

    /**
     * Map to the desired format and filter out negative measurements. After that we group our data by country
     * and we calculate the sum of populations for each country
     *
     */
    private static DataStream<Tuple2<String, Long>> groupByCountry(DataStream<String> kafkaData) {

        return kafkaData
                .map(new PopulationSplitter())            // Split data and keep only relevant attributes
                .filter(new FilterFunction<Tuple2<String, Long>>() {
                    @Override
                    public boolean filter(Tuple2<String, Long> value) throws Exception {
                        return (value.f1 >= 0);
                    }
                })
                .keyBy(0)            // groupBy country and parameter
                .timeWindow(Time.seconds(2))   // Add a window to compute strata in batches
                .reduce(new PopulationReducer())        // Reduce. New tuples will be at the format of Country, Parameter, Measurement, Measurement, Measurement, Count
                .setParallelism(4);
//                .map(new PopulationMapper());
    }



}
