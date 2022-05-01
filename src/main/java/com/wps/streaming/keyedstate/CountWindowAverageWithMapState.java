package com.wps.streaming.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import java.util.List;
import java.util.UUID;

public class CountWindowAverageWithMapState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Double>> {

    private MapState<String,Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                "average",
                String.class, Long.class
        );
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
        mapState.put(UUID.randomUUID().toString(),element.f1);

        List<Long> allElements = Lists.newArrayList(mapState.values());

        if (allElements.size() == 3) {
            long sum = 0;
            for (Long ele : allElements) {
                sum += ele;
            }
            double avg = (double) sum /3;
            out.collect(Tuple2.of(element.f0,avg));
            mapState.clear();
        }
    }
}
