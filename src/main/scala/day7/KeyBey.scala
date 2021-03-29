package day7

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object KeyBey {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
  env.readTextFile("E:\\IdeaProject\\flink\\src\\main\\Log\\b.txt")
      .filter(x=>x.nonEmpty)
      .flatMap(x=>{
        x.split("\\s+")
      }).map(x=>{
    (x,1)
  }).keyBy(0)
      .sum(1)
      .print()
    env.execute(this.getClass.getSimpleName)

  }
}
