package flink_second_pass;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.sampling.ReservoirSamplerWithoutReplacement;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Iterator;
import java.util.Properties;

public class SecondPassMain {
    public static void main(String[] args) throws Exception {

        // Setting up a streaming environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Setting the time characteristics for the windows implementation
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime); // Setting time characteristics for windowing.
        env.setParallelism(1);

        // Reading the initial data set into a stream and then split it.
        // NOTE: f1 = city, f2 = country, f5 = parameter, f6 = value, f7 = unit.
        DataStream<String> initDataS = env.readTextFile("openaqDataSet.csv");
        DataStream<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> initDataT = StringToTuples(initDataS);

        ///////// Reading output from first pass /////////
        // Reading output from first pass
        DataStream<String> firstPassInputS = env.readTextFile("output_fistPass");
        DataStream<Tuple3<String, String, Integer>> firstPassInputT = StrungToTuples2(firstPassInputS);

        // Joining the 2 streams, namely "initDataT" and "firstPassInputT"
        DataStream<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> joined = joinInput(initDataT,firstPassInputT);

        // Sampling the initial data set by stratum and printing the sampled tuples per stratum.
        DataStream<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> sampled_final = Sampler(joined);

        // Execute job

        env.execute("Second Pass Job");
    }
    private static  DataStream<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> StringToTuples(DataStream<String> initDataS){
            return initDataS
                    .map(new MapFunction<String, Tuple11<String, String, String, String, String, String, String, String, String, String, String>>() {
                        @Override
                        public Tuple11<String, String, String, String, String, String, String, String, String, String, String> map(String value) throws Exception {
                            String words[] = value.split(",");
                            return new Tuple11<>(words[0], words[1], words[2], words[3], words[4], words[5], words[6], words[7], words[8], words[9], words[10]);
                        }
                    });
    }

    private static  DataStream<Tuple3<String, String, Integer>> StrungToTuples2 (DataStream<String> firstPassInputS){
        return firstPassInputS.map(new MapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public Tuple3<String, String, Integer> map(String value) throws Exception {
                String valuesub = value.substring(1 , value.length()-1);  //Getting rid of the first and last character
                String words[] = valuesub.split(",");
                return new Tuple3<>(words[0], words[1], Integer.parseInt(words[4]));
            }
        });
    }

    private static  DataStream<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> joinInput(
           DataStream<Tuple11<String, String, String, String, String, String, String, String, String, String, String>> initDataT,
           DataStream<Tuple3<String, String, Integer>> firstPassInputT){
            return initDataT.join(firstPassInputT)
                    .where(new KeySelector<Tuple11<String, String, String, String, String, String, String, String, String, String, String>, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, String> getKey(Tuple11<String, String, String, String, String, String, String, String, String, String, String> value) throws Exception {
                            return new Tuple2<>(value.f2, value.f5);
                        }
                    })
                    .equalTo(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, String> getKey(Tuple3<String, String, Integer> value) throws Exception {
                            return new Tuple2<>(value.f0, value.f1);
                        }
                    })
                    .window(TumblingTimeWindows.of(Time.seconds(20)))
                    .apply(new JoinFunction<Tuple11<String, String, String, String, String, String, String, String, String, String, String>, Tuple3<String, String, Integer>, Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>>() {
                        @Override
                        public Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>
                        join(Tuple11<String, String, String, String, String, String, String, String, String, String, String> first, Tuple3<String, String, Integer> second) throws Exception {
                            return new Tuple12<>(first.f0, first.f1, first.f2, first.f3, first.f4, first.f5, first.f6, first.f7, first.f8, first.f9, first.f10, second.f2 );
                        }
                    });
    }
    private static  DataStream<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>>
    Sampler(DataStream<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> joined){
        return joined
                .keyBy(2, 5)
                .timeWindow(Time.seconds(20))
                .apply(new WindowFunction<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>, Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> input, Collector<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> out) throws Exception {
                        //Sampling
//                        Iterator<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> iterator = DataStreamUtils.collect(joined);
                        Iterator<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> iterator = input.iterator();
                        int numOfSamples = 0;
                        long count = 0;
                        for (Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer> in : input) {
                            numOfSamples = in.f11;
                            count++;
                            if (count > 1 && numOfSamples!=0) {
                                break;
                            }
                        }
                        ReservoirSamplerWithoutReplacement<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> reservoirSampler =
                                new ReservoirSamplerWithoutReplacement<>(numOfSamples);
                        // Now we create a sink for our sampled stream.
                        // Print our final stream in terminal.
                        Iterator<Tuple12<String, String, String, String, String, String, String, String, String, String, String, Integer>> sampled = reservoirSampler.sample(iterator);
                        System.out.println(tuple);
                        sampled.forEachRemaining(t -> System.out.println(t));
                    }
                });
    }
}
