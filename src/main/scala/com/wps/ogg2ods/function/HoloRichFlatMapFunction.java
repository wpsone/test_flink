package com.wps.ogg2ods.function;

import com.alibaba.fastjson.JSONObject;
import com.wps.ogg2ods.bean.SchemaOfOgg;
import com.wps.ogg2ods.utils.DateTimeUtil;
import com.wps.ogg2ods.utils.JdbcUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.logging.SimpleFormatter;

public class HoloRichFlatMapFunction extends RichFlatMapFunction<SchemaOfOgg, JSONObject> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOGGER = LoggerFactory.getLogger(HoloRichFlatMapFunction.class);
    private transient Connection holoConnect;
    private String tableName = null;
    private List<JSONObject> queryList;
    private Boolean is_put = false;

    @Override
    public void open(Configuration parameters) throws Exception {
        //在Flink算子中获取自定义参数
        Map<String, String> globalJobParametersMap = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();
        String myRunningWorkspace = globalJobParametersMap.get("running.workspace");
        tableName = globalJobParametersMap.get("ods.talbename");
        holoConnect = JdbcUtil.getHoloConnect(myRunningWorkspace);

        queryList = JdbcUtil.queryList(holoConnect,
                "SELECT nation_code,nation_name,database_and_table_name,old_column,new_column,holo_column_type FROM ods.ods_common_field_mapping WHERE database_and_table_name='"+tableName+"'",
                JSONObject.class,
                false);
        LOGGER.info("配置表数据："+queryList);
        Preconditions.checkArgument(!(queryList.size()==0),"未获取到配置表数据");
    }

    @Override
    public void flatMap(SchemaOfOgg sourceJsonObject, Collector<JSONObject> collector) throws Exception {
        JSONObject resultJsonObject = new JSONObject();
        String opType = sourceJsonObject.getOp_type();
        JSONObject after1 = sourceJsonObject.getAfter();
        if ("I".equals(opType)) {
            is_put = true;
            after1.put("is_delete_holo","否");
            sourceJsonObject.setAfter(after1);
        } else if ("U".equals(opType)) {
            for (JSONObject jsonObject : queryList) {
                if (!is_put) {//若之前判定不需要put
                    Object before = sourceJsonObject.getBefore().get(jsonObject.get("old_column"));
                    Object after = sourceJsonObject.getAfter().get(jsonObject.get("old_column"));
                    is_put = !(before==null?"null":before).equals(after==null?"null":after);
                    after1.put("is_delete_holo","否");
                    sourceJsonObject.setAfter(after1);
                }
            }
        } else {
            is_put = true;
            resultJsonObject.put("is_delete_holo","是");
        }

        if (is_put) {
            resultJsonObject.put("nation_code",sourceJsonObject.getNation_code());
            resultJsonObject.put("nation_name",sourceJsonObject.getNation_name());
            for (JSONObject jsonObject : queryList) {
                String holoColumnType = jsonObject.get("holo_column_type").toString();
                Object myDefault = null;
                switch (holoColumnType) {
                    case "bigint":
                        myDefault = 0;
                        break;
                    case "numeric(19,4)":
                    case "numeric(19,2)":
                        myDefault = 0.0d;
                        break;
                    case "TIMESTAMPTZ":
                        Date date = new Date(-28800000L);
                        myDefault = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(date);//即1970-01-01 00:00:00
                        break;
                    case "text":
                    default:
                        myDefault = "缺省";
                }

                Object myValue = null;
                if ("TIMESTAMPTZ".equals(holoColumnType)) {
                    Object oldColumn = sourceJsonObject.getAfter().get(jsonObject.get("old_column"));
                    if (oldColumn==null) {
                        myValue=myDefault;
                    } else {
                        myValue = DateTimeUtil.timeZoneConverter(oldColumn.toString(),sourceJsonObject.getNation_code());
                    }
                } else {
                    if (sourceJsonObject.getAfter().size()==0) {
                        LOGGER.info("after为空，源数据为："+sourceJsonObject);
                    }
                    myValue = sourceJsonObject.getAfter().get(jsonObject.get("old_column"));
                }
                resultJsonObject.put(jsonObject.get("new_column").toString(),(myValue==null || "null".equals(myValue))?myDefault:myValue)   ;
            }
        }

        //过滤选择字段before = after的数据
        if (resultJsonObject.size()!=0) {
            collector.collect(resultJsonObject);
        }
    }

    @Override
    public void close() throws Exception {
        holoConnect.close();
        LOGGER.info("连接关闭");
    }
}
