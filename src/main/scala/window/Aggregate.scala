package window

import day3.Raytek
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object Aggregate {
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
      }).keyBy("id")
      .timeWindow(Time.seconds(5))
        .aggregate(new AggregateFunction[Raytek,(String,Int,Double),(String,Double)] {

          override def createAccumulator(): (String, Int, Double) = {
            ("",0,0.0)
          }

          override def add(in: Raytek, acc: (String, Int, Double)): (String, Int, Double) = {
            (in.id,acc._2+1,acc._3+in.temperature)
          }



          override def merge(acc: (String, Int, Double), acc1: (String, Int, Double)): (String, Int, Double) = {
            (acc._1,acc._2+acc1._2,acc._3+acc._3)
          }

          override def getResult(acc: (String, Int, Double)): (String, Double) = {
            (acc._1,acc._3.toDouble/acc._2.toDouble)
          }

        }).global
      .print()

env.execute(this.getClass.getSimpleName)
  }
}
