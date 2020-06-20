package evan.wang.flink.examples.wordcount

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
  * Skeleton for a Flink Streaming Job.
  *
  * For a tutorial how to write a Flink streaming application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution, run
  * 'mvn clean package' on the command line.
  *
  * If you change the name of the main class (with the public static void main(String[] args))
  * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
  */
object StreamingJob {


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
