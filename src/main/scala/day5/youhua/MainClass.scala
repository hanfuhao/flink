package day5.youhua

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MainClass{
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.addSource(Mysource).print()
    env.execute()
  }
}
