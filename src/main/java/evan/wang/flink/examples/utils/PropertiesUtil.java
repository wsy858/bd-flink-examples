package evan.wang.flink.examples.utils;

import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public class PropertiesUtil {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesUtil.class);
    private static final Properties contextProperties = new Properties();
    static {
        //文件要放到resource文件夹下
        try(InputStream inputStreamCommon = Thread.currentThread().getContextClassLoader().getResourceAsStream("common.properties")){
            contextProperties.load(inputStreamCommon);
            if (CollectionUtils.isEmpty(contextProperties.entrySet())) throw new IllegalArgumentException("读取不到配置信息");
            logger.info("读取配置文件结束");
        }catch (Exception e){
            logger.error(e.getMessage(), e);
        }
    }

    public static Integer  getIntValue(String key){
        return Integer.parseInt(getStrValue(key));
    }

    public static Integer  getIntValue(String key, int defaultValue)  {
        String strValue = getStrValue(key);
        if (strValue == null) {
           return defaultValue;
        } else{
            return Integer.parseInt(strValue);
        }
    }

    public static String  getStrValue(String key){
       return contextProperties.getProperty(key);
    }

    public static Properties getKafkaProperties(String groupId){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"));
        properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"));
        properties.setProperty("group.id", groupId);
        return properties;
    }


}
