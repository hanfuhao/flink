package Sql

import day3.Raytek
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Tumble
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.sources.wmstrategies.BoundedOutOfOrderTimestamps
import org.apache.flink.types.Row

object sql3 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val envt=StreamTableEnvironment.create(env)
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    import org.apache.flink.table.api.scala._
env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val stream= env.socketTextStream("Hadoop2",8888)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[Raytek](Time.seconds(2)) {
      override def extractTimestamp(t: Raytek): Long = {
        t.time*1000
      }
    })



    val table= envt.fromDataStream(stream,'id,'wm.rowtime)
      .window(Tumble over 2.second on 'wm as 'tw)
      .groupBy('id,'tw)
      .select('id,'id.count)

    envt.toAppendStream[Row](table).print()

    env.execute()
  }
}
