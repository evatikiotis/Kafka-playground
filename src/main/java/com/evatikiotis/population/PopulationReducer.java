package com.evatikiotis.population;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PopulationReducer implements ReduceFunction<Tuple2<String, Long>>
{
    public Tuple2<String, Long> reduce(Tuple2<String, Long> pre_result,
                                          Tuple2<String,  Long> current)
    {
        return new Tuple2<>(current.f0, current.f1 + pre_result.f1);
    }
}
