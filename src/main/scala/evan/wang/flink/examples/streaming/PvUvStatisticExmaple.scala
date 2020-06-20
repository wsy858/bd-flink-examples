package evan.wang.flink.examples.streaming

import java.text.SimpleDateFormat
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

/**
  * 实时统计PV、UV
  */
object PvUvStatisticExmaple {
  val topicPrefix = "topic_flink_example_"

  case class Event(ip: String, createTime: String, count: Int = 1)

  /**
    * 源数据示例(ip简化为整数)： {"ip" : 0, "createTime" : "2019-05-24 18:02:26.292"}
    */
  def initKafkaProperties(): Properties = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "10.201.5.57:9092")
    properties.setProperty("group.id", "test")
    properties
  }

  def main(args: Array[String]): Unit = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    env.enableCheckpointing(5000)

    val properties = initKafkaProperties()
    // import org.apache.flink.api.scala._
    val kafkaSource = new FlinkKafkaConsumer[JSONObject](topicPrefix + "pv_uv_in", new JsonObjectDeserializationSchema(), properties)
    val stream = env.addSource(kafkaSource)
    val sink1 = new FlinkKafkaProducer[String](topicPrefix + "pv_uv_out", new SimpleStringSchema(), properties)
    sink1.setWriteTimestampToKafka(true)

    val sink2 = new FlinkKafkaProducer[String](topicPrefix + "pv_uv_out2", new SimpleStringSchema(), properties)
    sink2.setWriteTimestampToKafka(true)

    val ds = stream.map(node => Event(node.getString("ip"), node.getString("createTime")))
      .assignAscendingTimestamps(event => sdf.parse(event.createTime).getTime) //指定时间字段创建watermark,67
      .windowAll(TumblingEventTimeWindows.of(Time.seconds(10))) //数据集是KeyedStream类型则调用window方法，None-Keyed类型则调用windowsAll方法
      .reduce(new ReduceFunction[Event] {
      override def reduce(value1: Event, value2: Event): Event = {
        Event(value1.ip, null, value1.count + value2.count)
      }
    }).map(a => {
      val obj = new JSONObject()
      obj.put("ip", a.ip)
      obj.put("count", a.count)
      obj.toJSONString
    })

    ds.addSink(sink1)
    ds.addSink(sink2)

    env.execute("PvUv")
  }


}
