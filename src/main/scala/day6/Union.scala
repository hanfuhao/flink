package day6

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Union {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

  val stream= env.socketTextStream("Hadoop2",9999)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      }).split(x=>{
      if(x.temperature>36.3&&x.temperature<37.2)Seq("正常")
      else Seq("异常")
    })
    val normal=stream.select("正常")
    val unnormal=stream.select("异常")

    normal.union(unnormal).print()
    env.execute(this.getClass.getSimpleName)
  }
}
