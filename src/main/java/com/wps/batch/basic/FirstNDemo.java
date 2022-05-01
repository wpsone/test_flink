package com.wps.batch.basic;

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FirstNDemo {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<Tuple2<Integer,String>> data = new ArrayList<>();
        data.add(new Tuple2<>(2,"zs"));
        data.add(new Tuple2<>(4,"ls"));
        data.add(new Tuple2<>(3,"ww"));
        data.add(new Tuple2<>(1,"xw"));
        data.add(new Tuple2<>(1,"aw"));
        data.add(new Tuple2<>(1,"mw"));

        DataSource<Tuple2<Integer, String>> text = env.fromCollection(data);

        text.first(3).print();
        System.out.println("========================");

        text.groupBy(0).first(2).print();
        System.out.println("===========================");

        text.groupBy(0).sortGroup(1, Order.ASCENDING).first(2).print();
        System.out.println("========================");

        text.sortPartition(0,Order.ASCENDING).sortPartition(1,Order.DESCENDING).first(3).print();


    }
}
