package day5

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Max {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.socketTextStream("Hadoop2",8888)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      })
       .keyBy("id")
//        .minBy("temperature")
     .max("temperature")
     .print("迄今为止的最高温度->")
    env.execute(this.getClass.getSimpleName)
  }
}
