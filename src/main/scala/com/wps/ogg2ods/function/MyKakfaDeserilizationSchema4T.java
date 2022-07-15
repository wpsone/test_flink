package com.wps.ogg2ods.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.print.attribute.standard.MediaSize;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;


public class MyKakfaDeserilizationSchema4T<T> implements KafkaDeserializationSchema<T> {

    private Class<T> clazz;
    final Logger logger = LoggerFactory.getLogger(MyKakfaDeserilizationSchema4T.class);

    public MyKakfaDeserilizationSchema4T(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public boolean isEndOfStream(T t) {
        return false;
    }

    @Override
    public T deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(consumerRecord.value()).order(ByteOrder.LITTLE_ENDIAN);
        String myMessage = byteBuffertoString(buffer);

        JSONObject sourceJsonObject = JSON.parseObject(myMessage);

        String topicNation = consumerRecord.topic().split("_")[2];
        String nationCode = null;
        String nationName = null;

        switch (topicNation) {
            case "v9":
                nationCode = "VN";
                nationName = "越南";
                break;
            case "thai9":
                nationCode = "TH";
                nationName = "泰国";
                break;
            case "k9":
                nationCode = "KH";
                nationName = "柬埔寨";
                break;
            case "sin9":
                nationCode = "SG";
                nationName = "新加坡";
                break;
            case "my9":
                nationCode = "MY";
                nationName = "马来西亚";
                break;
            default:
                nationCode = "缺省";
                nationName = "缺省";
        }

        sourceJsonObject.put("nation_code",nationCode);
        sourceJsonObject.put("nation_name",nationName);

        if ("D".equals(sourceJsonObject.get("op_type"))) {
            sourceJsonObject.put("after",sourceJsonObject.get("before"));
        }

        return JSON.parseObject(sourceJsonObject.toJSONString(),clazz);
    }

    public static String byteBuffertoString(ByteBuffer buffer) {
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;

        try {
            charset = Charset.forName("UTF-8");
            decoder = charset.newDecoder();
//            charBuffer = decoder.decode(buffer);//用这个，只能输出一次结果，第二次显示为空
            charBuffer = decoder.decode(buffer.asReadOnlyBuffer());
            return charBuffer.toString();
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeExtractor.getForClass(clazz);
    }
}
