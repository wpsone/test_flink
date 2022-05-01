package com.wps.batch.basic;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class CounterDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("a", "b", "c", "d","e","f","g");
        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private IntCounter numLines = new IntCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("num-lines", this.numLines);
            }

            @Override
            public String map(String value) throws Exception {
                this.numLines.add(1);
                System.out.println("Map:"+numLines.getLocalValue());
                return value;
            }
        }).setParallelism(2);

        result.writeAsText("E:\\DataFiles\\kkb_data\\mycounter");
        JobExecutionResult jobResult = env.execute("counter");
        int num = jobResult.getAccumulatorResult("num-lines");
        System.out.println("num:"+num);
    }
}
