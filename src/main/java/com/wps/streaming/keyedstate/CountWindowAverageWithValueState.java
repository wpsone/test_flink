package com.wps.streaming.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * 从这个地方我们可以看出来，是比我们的SparkStreming要灵活。
 *
 * SparkStreaming = map(flatMap) + HBase(state)
 *
 *
 *  ValueState<T> ：这个状态为每一个 key 保存一个值
 *      value() 获取状态值
 *      update() 更新状态值
 *      clear() 清除状态
 */
public class CountWindowAverageWithValueState
        extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    // 用以保存每个 key 出现的次数，以及这个 key 对应的 value 的总值
    // managed keyed state
    //1. ValueState 保存的是对应的一个 key 的一个状态值
    private ValueState<Tuple2<Long, Long>> countAndSum;//key 1

    /**
     * long key出现的次数
     * long 存对应的这个key的值累计的value的值
     * 1,3  -> 1,3
     * 1，5 -> 2,8
     * 1,7  -> 3,15
     */


    //初始化的方法，这个方法只会运行一次。
    //我们之所以 用rich的接口目的就是用来注册状态的。
    @Override
    public void open(Configuration parameters) throws Exception {
        // 注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor =
                new ValueStateDescriptor<Tuple2<Long, Long>>(
                        "average",  // 状态的名字
                        Types.TUPLE(Types.LONG, Types.LONG)); // 状态存储的数据类型

        countAndSum = getRuntimeContext().getState(descriptor);
    }

    //1，3
    //1,5
    @Override
    public void flatMap(Tuple2<Long, Long> element,
                        Collector<Tuple2<Long, Double>> out) throws Exception {
        // 拿到当前的 key 的状态值
        Tuple2<Long, Long> currentState = countAndSum.value();

        // 如果状态值还没有初始化，则初始化
        if (currentState == null) {
            //count 0
            //sum 0
            currentState = Tuple2.of(0L, 0L);
        }
        //1,3
        //count =1
        //count =2
        // 更新状态值中的元素的个数
        currentState.f0 += 1;

        // 更新状态值中的总值
        //1,3
        //sum=3
        //sum=5
        currentState.f1 += element.f1;

        // 更新状态
        //state=1,3
        //state=2,5

        //state=3,15
        countAndSum.update(currentState);

        // 判断，如果当前的 key 出现了 3 次，则需要计算平均值，并且输出
        if (currentState.f0 == 3) {
            double avg = (double)currentState.f1 / currentState.f0;
            // 输出 key 及其对应的平均值
            out.collect(Tuple2.of(element.f0, avg));
            //  清空状态值
            countAndSum.clear();
        }
    }
}