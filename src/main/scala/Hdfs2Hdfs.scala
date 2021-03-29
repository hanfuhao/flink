import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem

object Hdfs2Hdfs {
  def main(args: Array[String]): Unit = {

    if(args==null || args.length!=4){
      println(
        """
          |警告！请录入参数！ --input <源的path> --output <目的地的path>
          |""".stripMargin)
      sys.exit(-1)
    }
val tool=ParameterTool.fromArgs(args)
    val inputPath=tool.get("input")
    val outputPath=tool.get("output")
    val env=ExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    env.readTextFile(inputPath)
      .flatMap(x=>x.split("\\s+"))
      .filter(x=>x.nonEmpty)
      .map(x=>(x,1))
      .groupBy(0)
      .sum(1)
      .writeAsText(outputPath,FileSystem.WriteMode.OVERWRITE)
      .setParallelism(1)

    env.execute(this.getClass.getSimpleName)

  }
}
