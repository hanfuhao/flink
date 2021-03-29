import org.apache.flink.api.scala.ExecutionEnvironment

object Local2Local {
  def main(args: Array[String]): Unit = {
    //获取环境，在这个地方用的是scala，可以选择java
    val env=ExecutionEnvironment.getExecutionEnvironment

    //导入隐士转换
    import org.apache.flink.api.scala._

    env.readTextFile("test/a.txt")
      .flatMap(x=>x.split("\\s+"))
      .filter(x=>x.nonEmpty)
      .map(x=>(x,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
