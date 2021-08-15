package evan.wang.flink.examples.practical

import org.apache.flink.streaming.api.scala._
import org.apache.flink.walkthrough.common.sink.AlertSink
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.source.TransactionSource

/**
 * 官网示例欺诈检测
 * https://ci.apache.org/projects/flink/flink-docs-release-1.13/zh/docs/try-flink/datastream/#final-application
 */
object FraudDetectionJob {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env : StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val transactions = env
      .addSource(new TransactionSource)
      .name("transactions")

    val alerts: DataStream[Alert] = transactions
      .keyBy(transaction => transaction.getAccountId)
      .process(new FraudDetector)
      .name("fraud-detector")

    alerts
      .addSink(new AlertSink)
      .name("send-alerts")

    env.execute("Fraud Detection")

  }

}



