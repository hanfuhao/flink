import org.apache.flink.api.scala.ExecutionEnvironment

object Hdfs2Local {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.readTextFile("hdfs://Hadoop3/flink/text")
      .flatMap(x=>x.split("\\s+"))
      .filter(x=>x.nonEmpty)
      .map(x=>(x,1))
      .groupBy(0)
      .sum(1)
      .print()
  }
}
