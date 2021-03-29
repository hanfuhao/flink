package day5

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Reduce {
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
      })

    stream.keyBy("id")
      .reduce((x:Raytek,y:Raytek)=>{
        val zuizao=Math.min(x.time,y.time)
        val tmp=Math.max(x.temperature,y.temperature)
        Raytek(x.id,tmp,"",zuizao,"")
      }).print("->")

    env.execute(this.getClass.getSimpleName)
  }
}
