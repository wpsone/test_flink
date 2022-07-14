package com.wps.ogg2ods.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {
    private static String getBrokers(String runningWorkspace) {
        String brokers = null;
        switch (runningWorkspace) {
            case "sea_flink_default":
                brokers = PropertyUtil.getProperty("pro.brokers");
                break;
            case "sea_flink_dev":
            default:
                brokers = PropertyUtil.getProperty("dev.brokers");
        }
        return brokers;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String runningWorkspace, String topic) {
        return new FlinkKafkaProducer<String>(getBrokers(runningWorkspace),topic,new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducer(String runningWorkspace, String topic, KafkaSerializationSchema<T> kafkaSerializationSchema) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,getBrokers(runningWorkspace));

        return new FlinkKafkaProducer<T>(topic,
                kafkaSerializationSchema,
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    public static FlinkKafkaConsumer<String> getProKafkaConsumer(String runningWorkspace,String topic,String groupId) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,getBrokers(runningWorkspace));

        return new FlinkKafkaConsumer<String>(topic,
                new SimpleStringSchema(),
                properties);
    }

    //拼接Kafka相关属性到DDL
    public static String getKakfaDDL(String runningWorkspace,String topic,String groupId) {
        return "'connector'='kafka', " +
                "'topic'='"+topic+"',"+
                "'properties.bootstrap.servers'='"+getBrokers(runningWorkspace)+"',"+
                "'properties.group.id'='"+groupId+"', " +
                "'format'='json', " +
                "'scan.startup.mode'='latest-offset' ";
    }

    public static Properties getKafkaConfig(String runningWorkspace,String groupId,String offsetType) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers",getBrokers(runningWorkspace));
        props.setProperty("group.id",groupId);
        props.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("session.timeout.ms","10000");
        props.setProperty("request.timeout.ms","10000");
        return props;
    }
}
