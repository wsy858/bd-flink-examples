package evan.wang.flink.examples.utils

import java.util.Properties

import org.apache.commons.collections.CollectionUtils
import org.slf4j.LoggerFactory

object PropertiesUtil {
  private val logger = LoggerFactory.getLogger("Property")
  private val contextProperties = new Properties()
  try {
    val inputStreamCommon = Thread.currentThread().getContextClassLoader.getResourceAsStream("common.properties") //文件要放到resource文件夹下
    contextProperties.load(inputStreamCommon)
    if (CollectionUtils.isEmpty(contextProperties.entrySet())) throw new IllegalArgumentException("读取不到配置信息")
    logger.info("读取配置文件结束")
  } catch {
    case exception: Exception => logger.error("读取配置文件异常", exception)
  }

  def getIntValue(key: String): Int = {
    val strValue = getStrValue(key)
    strValue.toInt
  }

  def getIntValue(key: String, defaultValue: Int): Int = {
    val strValue = getStrValue(key)
    if (strValue == null) {
      defaultValue
    } else
      strValue.toInt
  }

  def getStrValue(key: String): String = contextProperties.getProperty(key)

  def getKafkaProperties(groupId: String): Properties = {
    val properties = new Properties
    properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"))
    properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"))
    properties.setProperty("group.id", groupId)
    properties
  }

}
