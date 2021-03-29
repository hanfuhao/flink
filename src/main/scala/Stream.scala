import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Stream {
  def main(args: Array[String]): Unit = {
    if(args==null || args.length!=4){
      println(
        """
          |!警告，请输入端口号
          |""".stripMargin)
      sys.exit(-1)
    }
    val tool=ParameterTool.fromArgs(args)
    val hostname=tool.get("hostname")
    val port=tool.getInt("port")

    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.socketTextStream(hostname,port)
      .flatMap(x=>x.split("\\s+"))
      .filter(x=>x.nonEmpty)
      .map(x=>(x,1))

      .keyBy(0)
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)

  }
}
