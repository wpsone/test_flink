package com.wps.ogg2ods.utils;

import com.alibaba.fastjson.JSONObject;
import com.wps.ogg2ods.bean.TransientSink;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.java.utils.MultipleParameterTool;
import org.apache.flink.calcite.shaded.com.google.common.base.CaseFormat;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

public class JdbcUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcUtil.class);

    public static <T> List<T> queryList(Connection connection,String querySql,Class<T> clz,boolean underScoreToCamel) throws Exception {
        //创建集合用于存放查询结果数据
        List<T> resultList = new ArrayList<>();
        //预编译SQL
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        //执行查询
        ResultSet resultSet = preparedStatement.executeQuery();
        //解析resultSet
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        while (resultSet.next()) {
            //创建泛型对
            T t = clz.newInstance();
            //给泛型对象赋值
            for (int i = 1; i < columnCount + 1; i++) {
                //获取列名
                String columnName = metaData.getColumnName(i);
                //判断是否需要转换为驼峰命名
                if (underScoreToCamel) {
                    columnName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL,columnName.toLowerCase());
                }
                //获取列值
                Object value = resultSet.getObject(i);
                //给泛型对象赋值
                BeanUtils.setProperty(t,columnName,value);
            }
            //将该对象添加至集合
            resultList.add(t);
        }

        preparedStatement.close();
        resultSet.close();

        return resultList;
    }

    public static <T> SinkFunction<T> getSink(String sql) {
        return JdbcSink.sink(sql,
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        try {
                            //获取所有属性信息
                            Field[] fields = t.getClass().getDeclaredFields();

                            //遍历字段
                            int offset = 0;
                            for (int i = 0; i < fields.length; i++) {
                                //获取字段
                                Field field = fields[i];
                                //设置私有属性可访问
                                field.setAccessible(true);
                                //获取字段上注解
                                TransientSink annotation = field.getAnnotation(TransientSink.class);
                                if (annotation!=null) {
                                    //存在该注解
                                    offset++;
                                    continue;
                                }
                                //获取值
                                Object value = field.get(i);
                                //给预编译SQL对象赋值
                                preparedStatement.setObject(i+1-offset,value);
                            }
                        } catch (IllegalAccessException e) {
                            LOGGER.info("IllegalAccessException异常"+e.getMessage());
                        }
                    }
                },new JdbcExecutionOptions.Builder()
                    .withBatchSize(5)
                    .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withDriverName(PropertyUtil.getProperty("holo.jdbc.driver"))
                    .withUrl(PropertyUtil.getProperty("dev.holo.jdbc.url"))
                    .build());
    }

    public static Connection getHoloConnect(String runningWorkspace) {
        String holoJdbcUrl = null;
        switch (runningWorkspace) {
            case "sea_flink_default":holoJdbcUrl = PropertyUtil.getProperty("pro.holo.jdbc.url");break;
            case "sea_flink_dev":holoJdbcUrl = PropertyUtil.getProperty("dev.holo.jdbc.url");break;
            default:holoJdbcUrl=PropertyUtil.getProperty("dev.holo.jdbc.url");
        }
        try {
            Class.forName(PropertyUtil.getProperty("holo.jdbc.driver"));
        } catch (ClassNotFoundException e) {
            LOGGER.info("ClassNotFoundException异常"+e.getMessage());
        }
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(holoJdbcUrl);
        } catch (SQLException e) {
            LOGGER.info("SQLException异常"+e.getMessage());
        }
        return connection;
    }

    public static void main(String[] args) throws Exception {
        MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

        Properties properties = new Properties();

        String runningWorkspace = params.get("running.workspace");

        Connection holoConnect = getHoloConnect(runningWorkspace);

        List<JSONObject> queryList = queryList(holoConnect,
                "select field_mapping_key, nation_code, nation_name, database_and_table_name, old_column, new_column from ods.ods_common_field_mapping",
                JSONObject.class,
                true);

        for (JSONObject jsonObject : queryList) {
            LOGGER.info("info"+jsonObject.toJSONString());
        }
        holoConnect.close();
    }
}
