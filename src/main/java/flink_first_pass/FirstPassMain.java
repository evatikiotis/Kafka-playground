package flink_first_pass;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;


import java.util.Properties;

/**
 Group-by:Country,parameter and Calculate avg ,var, sd , si
 */
public class FirstPassMain {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // Creating Streaming Environment.
        env.setParallelism(3);
        // Setting the time characteristics for the windows implementation
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime); // Setting time characteristics for windowing.
        /*String bootsrtapServers = "127.0.0.1:9092";
        String topic = "Other";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootsrtapServers);
        // Add the kafka topic declared above as data source.
       // DataStream<String> kafkaData = streamEnvironment.addSource(new FlinkKafkaConsumer(topic, new SimpleStringSchema(), properties));*/
        DataStream<String> kafkaData = env.readTextFile("openaqDataSet.csv");
        DataStream<Tuple6<String, String, Double, Double, Double, Integer>> avgVarGamma = getAvgVarGamma(kafkaData);
        DataStream<Tuple2<Double, Integer>> toAggr = sumOfGammas(avgVarGamma);
        DataStream<Tuple3<Double, Integer, Integer>> totalGamma = toAggr.timeWindowAll(Time.seconds(20)).aggregate(new MyAggregate());
        DataStream<Tuple6<String, String, Double, Double, Double, Integer>> avgVarGammaToJoin = joinVarGamma(avgVarGamma);
        DataStream<Tuple5<String, String, Double, Double, Integer>> finalFirstPass = joinStreams(avgVarGammaToJoin, totalGamma);


        // Create a sink for our stream.
        finalFirstPass.print();
        finalFirstPass.writeAsText("output_fistPass").setParallelism(1);
        // Execute job
        env.execute("First Pass Job");
    }

    /**
     * Map to the desired format and filter out negative measurements. After that we group our data by country and
     * parameter in order to calculate the average, the variance and the gamma per stratum.
     */
    private static DataStream<Tuple6<String, String, Double, Double, Double, Integer>> getAvgVarGamma(DataStream<String> kafkaData) {

        return kafkaData
                .map(new Splitter())            // Split data and keep only relevant attributes
                .filter(new FilterFunction<Tuple5<String, String, Double, Double, Integer>>() {
                    @Override
                    public boolean filter(Tuple5<String, String, Double, Double, Integer> value) throws Exception {
                        return (value.f2 >= 0);
                    }
                })
                .keyBy(0, 1)            // groupBy country and parameter
                .timeWindow(Time.seconds(10))   // Add a window to compute strata in batches
                .reduce(new Reduce1())          // Reduce. New tuples will be at the format of Country, Parameter, Measurement, Measurement, Measurement, Count
//                .setParallelism(parallelism)
                .map(new Mapper());
    }

    /**
     * substream to calculate total gamma.
     */
    private static DataStream<Tuple2<Double, Integer>> sumOfGammas(DataStream<Tuple6<String, String, Double, Double, Double, Integer>> avgVarGamma) {

        return avgVarGamma.map(new MapFunction<Tuple6<String, String, Double, Double, Double, Integer>, Tuple2<Double, Integer>>() {
            @Override
            public Tuple2<Double, Integer> map(Tuple6<String, String, Double, Double, Double, Integer> value) throws Exception {
                return new Tuple2<>(value.f4, value.f5);
            }
        });
    }

    /**
     * Making a tranformation in order to be able to join the 2 streams
     */
    //
    private static DataStream<Tuple6<String, String, Double, Double, Double, Integer>> joinVarGamma(DataStream<Tuple6<String, String, Double, Double, Double, Integer>> avgVarGamma) {

        return avgVarGamma.map(new MapFunction<Tuple6<String, String, Double, Double, Double, Integer>, Tuple6<String, String, Double, Double, Double, Integer>>() {
            @Override
            public Tuple6<String, String, Double, Double, Double, Integer> map(Tuple6<String, String, Double, Double, Double, Integer> value) throws Exception {
                return new Tuple6<>(value.f0, value.f1, value.f2, value.f3, value.f4, 1);
            }
        });
    }

    /**
     * final stream Country,params,avg,var,sd, numberOfSumples(si)
     */
    private static DataStream<Tuple5<String, String, Double, Double, Integer>> joinStreams(DataStream<Tuple6<String, String, Double, Double, Double, Integer>> avgVarGammaToJoin,
                                                                                           DataStream<Tuple3<Double, Integer, Integer>> totalGamma){
        double memoryBudget = 0.01;
        return avgVarGammaToJoin.join(totalGamma)
                .where(new KeySelector<Tuple6<String, String, Double, Double, Double, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple6<String, String, Double, Double, Double, Integer> value) throws Exception {
                        return value.f5;
                    }
                })
                .equalTo(new KeySelector<Tuple3<Double, Integer, Integer>, Object>() {
                    @Override
                    public Object getKey(Tuple3<Double, Integer, Integer> value) throws Exception {
                        return value.f2;
                    }
                })
                .window(TumblingTimeWindows.of(Time.seconds(25)))
                .apply(new JoinFunction<Tuple6<String, String, Double, Double, Double, Integer>, Tuple3<Double, Integer, Integer>, Tuple5<String, String, Double, Double, Integer>>() {
                    @Override
                    public Tuple5<String, String, Double, Double, Integer> join(Tuple6<String, String, Double, Double, Double, Integer> first, Tuple3<Double, Integer, Integer> second) throws Exception {
                        int si = (int) Math.floor((first.f4 / second.f0) * (second.f1 * memoryBudget));
                        if (si >= 1.0) {
                            return new Tuple5<>(first.f0, first.f1, first.f2, first.f3, si);
                        } else {
                            return new Tuple5<>(first.f0, first.f1, first.f2, first.f3, 1);
                        }
                    }
                });

    }

}