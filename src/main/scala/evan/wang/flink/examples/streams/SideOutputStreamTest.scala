package evan.wang.flink.examples.streams

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector
import org.slf4j.{Logger, LoggerFactory}
import org.apache.flink.api.scala._

object SideOutputStreamTest {
  private val logger: Logger = LoggerFactory.getLogger(SideOutputStreamTest.getClass)

  lazy val webTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("Web端埋点数据")
  lazy val mobileTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("移动端埋点数据")
  lazy val csTerminal: OutputTag[MdMsg] = new OutputTag[MdMsg]("CS端埋点数据")

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      println("请输入socket主机和端口参数")
    }
    logger.info("----开始----")
    val host: String = args(0)
    val port: Int = args(1).toInt

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val socketData: DataStream[String] = env.socketTextStream(host, port)
    val outputStream: DataStream[MdMsg] = socketData.filter(_.split(",").length >= 3).map(line => {
      val str: Array[String] = line.split(",")
      MdMsg(str(0), str(1), str(2).toLong)
    }).process(new MdSplitProcessFunction)

    // Web端埋点数据流处理逻辑
    outputStream.getSideOutput(webTerminal).print("web")
    // Mobile端埋点数据流处理逻辑
    outputStream.getSideOutput(mobileTerminal).print("mobile")
    // CS端埋点数据流处理逻辑
    outputStream.getSideOutput(csTerminal).print("cs")

    env.execute("side output")
  }

  case class MdMsg(mdType: String, url: String, Time: Long)

  class MdSplitProcessFunction extends ProcessFunction[MdMsg, MdMsg] {
    override def processElement(value: MdMsg, ctx: ProcessFunction[MdMsg, MdMsg]#Context, out: Collector[MdMsg]): Unit = {
      // web
      if (value.mdType == "web") {
        ctx.output(webTerminal, value)
        // mobile
      } else if (value.mdType == "mobile") {
        ctx.output(mobileTerminal, value)
        // cs
      } else if (value.mdType == "cs") {
        ctx.output(csTerminal, value)
        // others
      } else {
        out.collect(value)
      }
    }
  }

}
