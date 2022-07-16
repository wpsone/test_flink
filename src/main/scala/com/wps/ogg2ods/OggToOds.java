package com.wps.ogg2ods;

import com.alibaba.fastjson.JSONObject;
import com.wps.ogg2ods.utils.JdbcUtil;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

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
        String odsTablename = (String) params.get("ods.tablename");
        Preconditions.checkNotNull(odsTablename,"请输入Holo中ods的表名，格式为--ods.tablename xxx");
        if (runningWorkspace.contains("_dev")) {
            Preconditions.checkArgument(odsTablename.contains("_dev"),"开发环境，ods.tablename参数末尾必须为_dev");
        }

        //验证ods输出表是否存在
        boolean is_contains_table = false;
        List<JSONObject> schemaJsonObjects = JdbcUtil.queryList(JdbcUtil.getHoloConnect(runningWorkspace),
                "SELECT CONCAT(table_namespace,',',\"table_name\") AS my_table_name FROM hologres.hg_table_properties WHERE table_namespace NOT IN ('hologres','hologres_statistic') AND property_key='storage_format' GROUP BY  table_namespace,\"table_name\" ORDER BY table_namespace,\"table_name\"",
                JSONObject.class,
                false);
        Preconditions.checkArgument(is_contains_table,"请先在Holo中正确的建表："+odsTablename);


    }
}
