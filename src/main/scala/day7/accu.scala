package day7

import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object accu {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.readTextFile("")
      .flatMap(_.split(","))
      .map(new RichMapFunction[(String),(String)] {

        private var totalAcc: IntCounter = _
        /*** 用于统计体温正常旅客数累加器 */
        private var normalAcc: IntCounter = _ /*** 用于统计体温异常旅客数累加器 */
        private var exceptionAcc: IntCounter = _

        override def open(parameters: Configuration): Unit = {
          //初始化累加器
          totalAcc = new IntCounter()
          normalAcc = new IntCounter()
          exceptionAcc = new IntCounter()
          //注册累加器
          val context: RuntimeContext = getRuntimeContext
          context.addAccumulator("totalAcc", totalAcc)
          context.addAccumulator("normalAcc", normalAcc)
          context.addAccumulator("exceptionAcc", exceptionAcc)

        }
        override def map(value: String): String = {

          ???
//
      }
      })
    var result: JobExecutionResult = env.execute()
    val dd=  result
    dd.getAccumulatorResult[Int]("totalAcc")
  }
}
