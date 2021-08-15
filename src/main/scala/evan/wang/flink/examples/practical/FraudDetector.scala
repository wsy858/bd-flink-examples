package evan.wang.flink.examples.practical

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
import org.apache.flink.walkthrough.common.entity.Alert
import org.apache.flink.walkthrough.common.entity.Transaction

object FraudDetector {
  val SMALL_AMOUNT: Double = 1.00
  val LARGE_AMOUNT: Double = 500.00
  val ONE_MINUTE: Long     = 60 * 1000L
}

@SerialVersionUID(1L)
class FraudDetector extends KeyedProcessFunction[Long, Transaction, Alert] {
  //状态
  @transient private var flagState: ValueState[java.lang.Boolean] = _
  //定时时间
  @transient private var timerState: ValueState[java.lang.Long] = _

  override def open(parameters: Configuration): Unit = {
    val flagDescription = new ValueStateDescriptor("flag", Types.BOOLEAN)
    flagState = getRuntimeContext.getState(flagDescription)

    val timerDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerDescriptor)
  }


  @throws[Exception]
  def processElement(
                      transaction: Transaction,
                      context: KeyedProcessFunction[Long, Transaction, Alert]#Context,
                      collector: Collector[Alert]): Unit = {
    // Get the current state for the current key
    val lastTransactionWasSmall = flagState.value
    // println(s"accountId: ${transaction.getAccountId}, amount: ${transaction.getAmount}")
    // Check if the flag is set
    if (lastTransactionWasSmall != null) {
      if (transaction.getAmount > FraudDetector.LARGE_AMOUNT) {
        // Output an alert downstream
        val alert = new Alert
        alert.setId(transaction.getAccountId)
        collector.collect(alert)
      }
      // Clean up our state
      cleanUp(context)
    }

    if (transaction.getAmount < FraudDetector.SMALL_AMOUNT) {
      // set the flag to true
      flagState.update(true)

      // set the timer and timer state
      val timer = context.timerService.currentProcessingTime + FraudDetector.ONE_MINUTE
      context.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }

  }

  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[Long, Transaction, Alert]#OnTimerContext,
                       out: Collector[Alert]): Unit ={
    // remove flag after 1 minute
    timerState.clear()
    flagState.clear()
  }


  @throws[Exception]
  private def cleanUp(ctx: KeyedProcessFunction[Long, Transaction, Alert]#Context): Unit = {
    // delete timer
    val timer = timerState.value
    ctx.timerService.deleteProcessingTimeTimer(timer)

    // clean up all states
    timerState.clear()
    flagState.clear()
  }


}
