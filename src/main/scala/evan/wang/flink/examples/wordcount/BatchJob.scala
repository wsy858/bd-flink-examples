package evan.wang.flink.examples.wordcount

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.core.fs.FileSystem.WriteMode

/**
  * Skeleton for a Flink Batch Job.
  *
  * For a tutorial how to write a Flink batch application, check the
  * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
  *
  * To package your application into a JAR file for execution,
  * change the main class in the POM.xml file to this class (simply search for 'mainClass')
  * and run 'mvn clean package' on the command line.
  */
object BatchJob {

  def main(args: Array[String]) {
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
