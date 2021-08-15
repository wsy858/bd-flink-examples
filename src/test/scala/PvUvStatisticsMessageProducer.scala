import java.text.SimpleDateFormat
import java.util.{Date, Properties, Random}

import com.alibaba.fastjson.JSONObject
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

object PvUvStatisticsMessageProducer {

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", args(1) + ":" + args(1)) // ip:port
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val producer: KafkaProducer[String, String] = new KafkaProducer(props)
    var i = 0
    while (i < 100) {
      val key = "key_" + (i % 4)
      val obj = new JSONObject()
      obj.put("ip", new Random().nextInt(10))
      obj.put("createTime", sdf.format(new Date()))
      val message = obj.toJSONString
      println(message)
      val record = new ProducerRecord[String, String]("topic_flink_example_pv_uv_in", key, message)
      //同步发送，调用get()则将阻塞，直到相关请求完成并返回该消息的metadata，或抛出发送异常
      producer.send(record)
      i += 1
      Thread.sleep(1000)
    }
  }

}
