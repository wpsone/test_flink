package com.wps.ogg2ods.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * properties文件获取工具类
 */
public class PropertyUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(PropertyUtil.class);
    private static Properties props;

    static {
        loadProps();
    }

    synchronized static private void loadProps() {
        System.out.println("开始加载properties文件内容......");
        props = new Properties();
        InputStream in = null;
        try {
            //第一种，通过类加载器进行获取properties文件流
            in = PropertyUtil.class.getClassLoader().getResourceAsStream("myproperty.properties");
            //第二种，通过类进行获取properties文件流
//            in = PropertyUtil.class.getResourceAsStream("/jdbc.properties");
            props.load(in);
        } catch (FileNotFoundException e) {
            LOGGER.info("jdbc.properties文件未找到");
        } catch (IOException e){
            LOGGER.info("出现IOException"+e);
        } finally {
            try {
                if (null != in) {
                    in.close();
                }
            } catch (IOException e) {
                LOGGER.info("jdbc.properties文件流关闭出现异常");
            }
        }
        LOGGER.info("加载properties文件内容完成...........");
    }

    /**
     * 根据key获取配置文件中的属性
     * @param key
     * @return
     */
    public static String getProperty(String key) {
        if (null == props) {
            loadProps();
        }
        return props.getProperty(key);
    }

    /**
     * 根据key获取配置文件中的属性，当为null时返回指定的默认值
     * @param key
     * @param defaultValue
     * @return
     */
    public static String getProperty(String key,String defaultValue) {
        if (null == props) {
            loadProps();
        }
        return props.getProperty(key,defaultValue);
    }
}
