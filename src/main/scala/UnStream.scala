import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object UnStream {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
  val dd=  env.readTextFile("E:\\IdeaProject\\flink\\src\\main\\scala\\Hdfs2Hdfs.scala")
    dd.print()
    env.execute(this.getClass.getSimpleName)
  }
}
