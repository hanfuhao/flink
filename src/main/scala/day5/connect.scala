package day5

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object connect {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val stream= env.socketTextStream("Hadoop2",8888)
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
   val normal=   stream.select("正常").map(x=>(x.id,x.name))
   val unnormal=  stream.select("异常").map(x=>(x.id,x.name,x.temperature))

    normal.connect(unnormal).map(x=>("红外测温仪"+x._1,"祝你旅途愉快"),
      y=>("红外测温仪"+y._1,y._2+"你的温度是"+y._3,"你的温度异常，即将被隔离")
    ).print()

    env.execute(this.getClass.getSimpleName)
  }
}
