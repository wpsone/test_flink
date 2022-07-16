package com.wps.ogg2ods;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.hologres.client.HoloClient;
import com.alibaba.hologres.client.HoloConfig;
import com.alibaba.hologres.client.Put;
import com.alibaba.hologres.client.exception.HoloClientException;
import com.alibaba.hologres.client.model.WriteMode;
import com.wps.ogg2ods.bean.SchemaOfOgg;
import com.wps.ogg2ods.function.HoloRichFlatMapFunction;
import com.wps.ogg2ods.function.MyKakfaDeserilizationSchema4T;
import com.wps.ogg2ods.utils.JdbcUtil;
import com.wps.ogg2ods.utils.MyKafkaUtil;
import com.wps.ogg2ods.utils.PropertyUtil;
import com.wps.ogg2ods.utils.ThreadPoolUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Preconditions;
import org.postgresql.model.TableSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

public class OggToOds {
    public static void main(String[] args) throws Exception {
        final Logger LOGGER = LoggerFactory.getLogger(OggToOds.class);
        final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        //获取运行环境
        String runningWorkspace = (String) params.get("running.workspace");
        Preconditions.checkNotNull(runningWorkspace,"请输入你所运行的工作空间，格式为--running.workspace xx");
        //获取消费的ogg-topic
        String oggTopics = (String) params.get("ogg.topic");
        Preconditions.checkNotNull(oggTopics,"请输入数据源topic，格式为--ogg.topic xxx;xxx");
        //获取输出的ods-topic
        String odsTopic = (String) params.get("ods.topic");
        Preconditions.checkNotNull(odsTopic,"请输入ods.topic，格式为--ods.topic xxx");
        if (runningWorkspace.contains("_dev")) {
            Preconditions.checkArgument(odsTopic.contains("_dev"),"开发环境，ods.topic参数末尾必须为_dev");
        }
        //获取ogg-topic消费组id
        String groupId = (String) params.get("group.id");
        Preconditions.checkNotNull(groupId,"请输入此Flink任务此次运行所使用的group.id，格式为--group.id xxx");
        if (runningWorkspace.contains("_dev")) {
            Preconditions.checkArgument(groupId.contains("_dev"),"开发环境，group.id参数末尾必须为_dev");
        }
        //获取ods输出表表名
        final String[] odsTablename = {(String) params.get("ods.tablename")};
        Preconditions.checkNotNull(odsTablename[0],"请输入Holo中ods的表名，格式为--ods.tablename xxx");
        if (runningWorkspace.contains("_dev")) {
            Preconditions.checkArgument(odsTablename[0].contains("_dev"),"开发环境，ods.tablename参数末尾必须为_dev");
        }

        //验证ods输出表是否存在
        boolean is_contains_table = false;
        List<JSONObject> schemaJsonObjects = JdbcUtil.queryList(JdbcUtil.getHoloConnect(runningWorkspace),
                "SELECT CONCAT(table_namespace,',',\"table_name\") AS my_table_name FROM hologres.hg_table_properties WHERE table_namespace NOT IN ('hologres','hologres_statistic') AND property_key='storage_format' GROUP BY  table_namespace,\"table_name\" ORDER BY table_namespace,\"table_name\"",
                JSONObject.class,
                false);
        Preconditions.checkArgument(is_contains_table,"请先在Holo中正确的建表："+ odsTablename[0]);

        for (JSONObject schemaJsonObject : schemaJsonObjects) {
            if (!is_contains_table) {
                is_contains_table = odsTablename[0].equals(schemaJsonObject.get("my_table_name"));
            } else {
                break;
            }
        }
        Preconditions.checkArgument(is_contains_table,"请先在Holo中正确的建表："+ odsTablename[0]);

        Configuration configuration = new Configuration();
        configuration.setString("running.workspace",runningWorkspace);
        configuration.setString("ods.tablename", odsTablename[0]);

        //环境初始化
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        //启用checkpoint,这里我没有对消息体的key value进行判断,即使为空启动了checkpoint,遇到错误也会无限次重启
        senv.enableCheckpointing(20001  );

        senv.getConfig().setGlobalJobParameters(configuration);

        List<String> oggTopicList = Arrays.asList(oggTopics.split(";"));
        FlinkKafkaConsumer<SchemaOfOgg> flinkKafkaConsumer = new FlinkKafkaConsumer<SchemaOfOgg>(
                oggTopicList,
                new MyKakfaDeserilizationSchema4T(SchemaOfOgg.class),
                MyKafkaUtil.getKafkaConfig("sea_flink_default", groupId, params.get("scan.startup.timestamp-millis"))
        );

        if (!(params.get("scan.startup.timestamp-millis")==null)) {
            flinkKafkaConsumer.setStartFromTimestamp(Long.parseLong(params.get("scan.startup.timestamp-millis")));
        } else {
            flinkKafkaConsumer.setStartFromEarliest();
            LOGGER.info("Kafka消费模式设定：Earliest");
        }

        //Source
        SingleOutputStreamOperator<JSONObject> oggSourceDS = senv.addSource(flinkKafkaConsumer).flatMap(new HoloRichFlatMapFunction());
        //配置flatMap算子并发
        if (params.get("ods.flatMap.parallelism")!=null) {
            oggSourceDS.setParallelism(Integer.parseInt(params.get("ods.flatMap.parallelism")));
            LOGGER.info("已配置flatMap并发："+params.get("ods.flatMap.parallelism"));
        }

        SingleOutputStreamOperator<JSONObject> sinkHolo4KafkaDS = oggSourceDS.map(new RichMapFunction<JSONObject, JSONObject>() {
            private String odsTableName = null;
            private ThreadPoolExecutor threadPoolExecutor;
            private HoloClient holoClient = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                Map<String, String> globalJobParametersMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
                String myRunningWorkspace = globalJobParametersMap.get("running.workspace");
                odsTableName = globalJobParametersMap.get("ods.tablename");

                HoloConfig holoConfig = new HoloConfig();
                switch (myRunningWorkspace) {
                    case "sea_flink_default":
                        holoConfig.setJdbcUrl(PropertyUtil.getProperty("pro.holo.url"));
                        holoConfig.setUsername(PropertyUtil.getProperty("pro.holo.username"));
                        holoConfig.setPassword(PropertyUtil.getProperty("pro.holo.password"));
                        break;
                    case "sea_flink_dev":
                    default:
                        holoConfig.setJdbcUrl(PropertyUtil.getProperty("dev.holo.url"));
                        holoConfig.setUsername(PropertyUtil.getProperty("dev.holo.username"));
                        holoConfig.setPassword(PropertyUtil.getProperty("dev.holo.password"));
                }
                /*
                当INSERT目标表为有主键的表时采用不同策略：
                INSERT_OR_IGNORE，当主键冲突时，不写入；
                INSERT_OR_UPDATE，当主键冲突时，更新相应列；
                INSERT_OR_REPLACE，默认值，当主键冲突时，更新所有列。
                 */
                holoConfig.setWriteMode(WriteMode.INSERT_OR_UPDATE);//配置主键冲突时策略
                holoConfig.setWriteThreadSize(8);
                holoConfig.setWriteBatchSize(800);//批量800条提交一次
                holoConfig.setFlushMaxWaitMs(5000L);//默认值60s
                try {
                    holoClient = new HoloClient(holoConfig);
                } catch (HoloClientException e) {
                    LOGGER.error("无法连接Holo", e);
                }
                threadPoolExecutor = ThreadPoolUtil.getThreadPool();
            }

            @Override
            public JSONObject map(JSONObject jsonObject) throws Exception {
                if (jsonObject.size() != 0) {
                    threadPoolExecutor.submit(new Runnable() {
                        @Override
                        public void run() {
                            try {
                                TableSchema schema0 = holoClient.getTableSchema(odsTableName);
                                Put put = new Put(schema0);
                                for (String key : jsonObject.keySet()) {
                                    put.setObject(key, jsonObject.get(key));
                                }
                            } catch (HoloClientException e) {
                                LOGGER.info("往holo的表" + odsTableName + "中所put数据" + jsonObject.toJSONString() + "时异常:" + e.getMessage());
                            }
                        }
                    });
                }
                return jsonObject;
            }

            @Override
            public void close() throws Exception {
                holoClient.close();
            }
        }).name("sink_holo4kafka").uid("sink_holo4kafka");

        SingleOutputStreamOperator<String> sinkKafkaDS = sinkHolo4KafkaDS.map(new MapFunction<JSONObject, String>() {
            @Override
            public String map(JSONObject jsonObject) throws Exception {
                return jsonObject.toJSONString();
            }
        }).name("sink_kafka").uid("sink_kafka");

        sinkKafkaDS.addSink(MyKafkaUtil.getKafkaProducer(runningWorkspace,odsTopic));
        senv.execute("oggToOds");
    }
}
