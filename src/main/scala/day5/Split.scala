package day5

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Split {
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
      if(x.temperature<37.3 &&x.temperature>36.3)Seq("正常")
      else Seq("异常")
    })
    stream.select("正常").print("体温正常->")
    stream.select("异常").print("体温异常->")

    env.execute(this.getClass.getSimpleName)
  }
}
