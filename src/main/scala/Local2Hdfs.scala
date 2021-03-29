import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

object Local2Hdfs {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.readTextFile("test/a.txt")
      .flatMap(x=>x.split("\\s+"))
      .filter(x=>x.nonEmpty)
      .map(x=>(x,1))
      .groupBy(0)
      .sum(1)
      .writeAsText("hdfs://Hadoop3/flink/text",FileSystem.WriteMode.OVERWRITE)

      env.execute(this.getClass.getSimpleName)
  }
}
