package sql2

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object sql1 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tenv=StreamTableEnvironment.create(env)
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._

  val ds=  env.socketTextStream("Hadoop2",8888)
      .filter(x=>x.trim.nonEmpty)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      })
   val table= tenv.fromDataStream(ds)
    tenv.sqlQuery(
      s"""
        |select
        |*
        |from
        |$table
        |where temperature<37.3 and temperature>36.2 and name like '%o%'
        |""".stripMargin).toAppendStream[Row].print()
    //  .toRetractStream[Row].print()
env.execute()
  }
}
