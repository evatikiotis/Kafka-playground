package flink_first_pass;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class Splitter implements MapFunction<String, Tuple5<String, String, Double, Double, Integer>>
{
    public Tuple5<String, String, Double, Double, Integer> map(String value) {
        String[] words = value.split(",");
        // Get the squares of the measurements in order to compute the variance
        return new Tuple5<>(words[2], words[5], Double.parseDouble(words[6]), Math.pow(Double.parseDouble(words[6]),2), 1);
    }
}