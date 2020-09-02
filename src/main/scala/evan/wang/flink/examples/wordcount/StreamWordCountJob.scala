package evan.wang.flink.examples.wordcount

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.slf4j.{Logger, LoggerFactory}

/**
  * a Streaming Job
  * nc -lk 9001  启动socket发送消息
  */
object StreamWordCountJob {
  private val logger: Logger = LoggerFactory.getLogger(StreamWordCountJob.getClass)

  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    if (args.length < 2) {
      println("请输入socket主机和端口参数")
    }
    logger.info("----开始----")
    val host: String = args(0)
    val port: Int = args(1).toInt
    val text = env.socketTextStream(host, port)
    val wordCounts = text.flatMap(_.split("\\s"))
      .filter(_.nonEmpty)
      .map(w => WordWithCount(w, 1))
      .keyBy("word") // .keyBy(0)
      //  .timeWindow(Time.seconds(5), Time.seconds(1))
      .sum("count") //  .sum(1)
      .setParallelism(1)

    wordCounts.print()
    // execute program
    env.execute("Flink Streaming Scala API Skeleton")
  }

  case class WordWithCount(word: String, count: Long)

}
