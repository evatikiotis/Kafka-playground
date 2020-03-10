package flink_first_pass;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

/** Calculate total Gamma */
public class MyAggregate
        implements AggregateFunction<Tuple2<Double, Integer>, Tuple2<Double, Double>, Tuple3<Double, Integer, Integer>> {
    @Override
    public Tuple2<Double, Double> createAccumulator() {
        return new Tuple2<>(0.0, 0.0);
    }

    @Override
    public Tuple2<Double, Double> add(Tuple2<Double, Integer> value, Tuple2<Double, Double> accumulator) {
        return new Tuple2<>(accumulator.f0 + value.f0, accumulator.f1 + value.f1);
    }

    @Override
//    public Double getResult(Tuple2<Double, Double> accumulator) {
//        return ((double) accumulator.f0);
//    }
    public Tuple3<Double, Integer, Integer> getResult(Tuple2<Double, Double> accumulator) {
        return new Tuple3<>(accumulator.f0, (int)(double)(accumulator.f1), 1);
    }

    @Override
    public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
