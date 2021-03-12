package com.evatikiotis.population;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class PopulationMapper implements MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {


    @Override
    public Tuple2<String, Integer> map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        return stringIntegerTuple2;
    }
}
