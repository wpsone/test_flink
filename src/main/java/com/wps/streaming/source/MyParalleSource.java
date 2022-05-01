package com.wps.streaming.source;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

/**
 * 功能：自定义支持并行度的数据源
 * 每秒产生一条数据
 */
public class MyParalleSource implements ParallelSourceFunction<Long> {
    private long number=1L;
    private boolean isRunning=true;
    @Override
    public void run(SourceContext<Long> sct) throws Exception {
        while (isRunning) {
            sct.collect(number);
            number++;
            //每秒生成一条数据
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning=false;
    }
}
