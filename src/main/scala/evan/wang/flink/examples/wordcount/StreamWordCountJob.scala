package evan.wang.flink.examples.wordcount

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._

/**
  * a Streaming Job
  * nc -lk 9001  启动socket发送消息
  */
object StreamWordCountJob {


  def main(args: Array[String]): Unit = {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("x.x.x.x", 9001)

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
