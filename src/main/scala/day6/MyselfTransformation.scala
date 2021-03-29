package day6

import day3.Raytek
import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object MyselfTransformation {
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
      }).filter(new Myfilter)
        .print()

    env.execute()
  }
  class Myfilter extends FilterFunction[Raytek] {
    override def filter(t: Raytek): Boolean = {
      if("raytek_2".equals(t.id)) true
      else false
    }
  }
}
