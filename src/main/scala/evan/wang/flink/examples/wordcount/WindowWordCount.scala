package evan.wang.flink.examples.wordcount

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
  *
  */
object WindowWordCount {


  def main(args: Array[String]) {
    // set up the streaming execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val text = env.socketTextStream("10.201.5.224", 9001)

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
