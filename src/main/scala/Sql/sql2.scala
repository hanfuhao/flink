package Sql

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object sql2 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val envt=StreamTableEnvironment.create(env)
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

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
   val table= envt.fromDataStream(stream,'i,'t)
      .select("i,t")
    envt.toAppendStream[Row](table).print()

    env.execute()
  }
}
