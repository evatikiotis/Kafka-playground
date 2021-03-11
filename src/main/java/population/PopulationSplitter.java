package population;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;


public class PopulationSplitter implements MapFunction<String, Tuple2<String, Long>>
{
    public Tuple2<String, Long> map(String value) {
        String[] words = value.split(",");
        // Get the squares of the measurements in order to compute the variance
        return new Tuple2<>(words[0], Long.parseLong(words[1]));
    }
}
