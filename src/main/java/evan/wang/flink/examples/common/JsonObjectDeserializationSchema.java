package evan.wang.flink.examples.common;

import static org.apache.flink.api.java.typeutils.TypeExtractor.getForClass;

import com.alibaba.fastjson.JSONObject;
import java.nio.charset.Charset;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonObjectDeserializationSchema implements DeserializationSchema<JSONObject> {
    private static final Logger logger =
            LoggerFactory.getLogger(JsonObjectDeserializationSchema.class);

    @Override
    public JSONObject deserialize(byte[] message) {
        JSONObject node = new JSONObject();
        try {
            node = JSONObject.parseObject(new String(message, Charset.forName("UTF-8")));
        } catch (Exception e) {
            logger.error("解析JSON消息异常", e);
        }
        return node;
    }

    @Override
    public boolean isEndOfStream(JSONObject nextElement) {
        return false;
    }

    @Override
    public TypeInformation<JSONObject> getProducedType() {
        return getForClass(JSONObject.class);
    }
}
