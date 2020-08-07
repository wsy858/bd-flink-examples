package evan.wang.flink.examples.wordcount

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * a Batch Job
  */
object BatchWordCount {

  def main(args: Array[String]): Unit = {
    // set up the batch execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    val wordCounts = env.readTextFile("E:\\code\\study\\FinkDemo1\\pom.xml")
      .flatMap(_.toLowerCase.split("\\s"))
      .filter(_.nonEmpty)
      .map((_, 1))
      .groupBy(0)
      .sum(1)
      .setParallelism(1)
      .sortPartition(1, Order.DESCENDING)
   // wordCounts.print()
    wordCounts.writeAsCsv("E:\\code\\study\\FinkDemo1\\target\\csv\\t.csv", "\n", ",", WriteMode.OVERWRITE)

    /**
      * 如果输出只有print，则不需要execute，如果有其它新的sink，则需要last call（execute...）
      * execute program
      * No new data sinks have been defined since the last execution. The last execution refers to the latest call to 'execute()', 'count()', 'collect()', or 'print()'
      */
    env.execute("Flink Batch Scala API Skeleton")
  }
}
