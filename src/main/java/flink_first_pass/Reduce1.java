package flink_first_pass;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class Reduce1 implements ReduceFunction<Tuple5<String, String, Double, Double, Integer>>
{
    public Tuple5<String, String, Double, Double, Integer> reduce(Tuple5<String, String, Double, Double, Integer> pre_result,
                                                                  Tuple5<String, String, Double, Double, Integer> current)
    {
        return new Tuple5<>(current.f0, current.f1, current.f2 + pre_result.f2, current.f3 + pre_result.f3, current.f4 + pre_result.f4);
    }
}
