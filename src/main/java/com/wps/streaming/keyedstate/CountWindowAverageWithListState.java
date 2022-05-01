package com.wps.streaming.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import java.util.Collections;
import java.util.List;

public class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Double>> {

    //1.ListState 保存的是对应的一个key出现的所有元素
    private ListState<Tuple2<Long,Long>> elementsByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ListStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ListStateDescriptor<Tuple2<Long, Long>>(
                        "average",  // 状态的名字
                        Types.TUPLE(Types.LONG, Types.LONG)); // 状态存储的数据类型
        elementsByKey = getRuntimeContext().getListState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
        Iterable<Tuple2<Long, Long>> currentState = elementsByKey.get();

        if (null == currentState) {
            elementsByKey.addAll(Collections.emptyList());
        }

        elementsByKey.add(element);
        List<Tuple2<Long,Long>> allElements = Lists.newArrayList(elementsByKey.get());

        if (allElements.size() == 3) {
            long count = 0;
            long sum = 0;
            for (Tuple2<Long, Long> ele : allElements) {
                count++;
                sum+=ele.f1;
            }
            double avg = (double) sum/count;
            out.collect(Tuple2.of(element.f0,avg));
            elementsByKey.clear();
        }
    }
}
