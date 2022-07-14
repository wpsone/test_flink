package com.wps.ogg2ods.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateTimeUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeUtil.class);
    private final static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");
    private final static ZoneId vn = ZoneId.of("Asia/Ho_Chi_Minh");//越南---东七区
    private final static ZoneId th = ZoneId.of("Asia/Bangkok");//泰国---东七区
    private final static ZoneId kh = ZoneId.of("Asia/Phnom_Penh");//柬埔寨---东七区
    private final static ZoneId sg = ZoneId.of("Asia/Singapore");//新加坡---东八区
    private final static ZoneId my = ZoneId.of("Asia/Kuala_Lumpur");//马来---东八区

    private static ZoneId zoneId = sg;//默认为新加坡---东八区
    private final static ZoneOffset zoneOffset7 = ZoneOffset.of("+7");
    private final static ZoneOffset zoneOffset8 = ZoneOffset.of("+8");
    private static ZoneOffset zoneOffset = zoneOffset8;//默认东八区

    public static String toYMDhms(Date date,String nationCode) {
        switch (nationCode) {
            case "VN": zoneId = vn;break;
            case "TH": zoneId = th;break;
            case "KH": zoneId = kh;break;
            case "SG": zoneId = sg;break;
            case "MY": zoneId = my;break;
            default: zoneId = sg;
        }
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(),zoneId);
        return formatter.format(localDateTime);
    }

    public static Long toTs(String YmDHms,String nationCode) {
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms,formatter);
        switch (nationCode) {
            case "VN": zoneOffset = zoneOffset7;break;
            case "TH": zoneOffset = zoneOffset7;break;
            case "KH": zoneOffset = zoneOffset7;break;
            case "SG": zoneOffset = zoneOffset8;break;
            case "MY": zoneOffset = zoneOffset8;break;
            default: zoneOffset = zoneOffset8;
        }
        return localDateTime.toInstant(zoneOffset).toEpochMilli();
    }

    public static String timeZoneConverter(String YmDHms,String nationCode) {
        DateTimeFormatter oggFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS");
        if (YmDHms == null) {
            LOGGER.info("日期字段捕获一个空值："+nationCode);
        }
        LocalDateTime localDateTime = LocalDateTime.parse(YmDHms, oggFormatter);
        switch (nationCode) {
            case "VN": zoneOffset = ZoneOffset.of("+9");break;
            case "TH": zoneOffset = ZoneOffset.of("+9");break;
            case "KH": zoneOffset = ZoneOffset.of("+9");break;
            case "SG": zoneOffset = zoneOffset8;break;
            case "MY": zoneOffset = zoneOffset8;break;
            default: zoneOffset = zoneOffset8;
        }
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return simpleDateFormat.format(localDateTime.toInstant(zoneOffset).toEpochMilli());
    }
}
