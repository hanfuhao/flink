package sql2

import day3.Raytek
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.types.Row

object sql3RowTime {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val tenv=StreamTableEnvironment.create(env)
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
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
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Raytek](Time.seconds(2)) {
      override def extractTimestamp(t: Raytek): Long ={
        t.time.toLong*1000
      }
    })

    val table=  tenv.fromDataStream(ds,'id,'wm.rowtime)
    tenv.sqlQuery(
      s"""
         |select
         |id,count(1)
         |from $table
         |group by id,tumble(wm,interval '2' second)
         |""".stripMargin)
      .toAppendStream[Row].print()

    env.execute()

  }
}
