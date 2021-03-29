package BroadCast

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


object BoundBroacast {

    def main(args: Array[String]): Unit = {
      val env=StreamExecutionEnvironment.getExecutionEnvironment
      import org.apache.flink.api.scala._

      val value1: DataStream[(Int, Char)] = env.fromElements((1, '男'), (2, '女'))
      val ds1=value1
    val ds2=  env.socketTextStream("Hadoop2", 9999)
        .filter(_.trim.nonEmpty)
        .map(perLine => {
          val arr = perLine.split(",")
          val id = arr(0).trim.toInt
          val name = arr(1).trim
          val genderFlg = arr(2).trim.toInt
          val address = arr(3).trim
          (id, name, genderFlg, address)
        })

    val broadcastInfo:MapStateDescriptor[Integer,Character]=  new MapStateDescriptor("broadcast",BasicTypeInfo.INT_TYPE_INFO,
        BasicTypeInfo.CHAR_TYPE_INFO
      )
      val value: BroadcastStream[(Int, Char)] = ds1.broadcast(broadcastInfo)
      val bc=value

    ds2.connect(bc).process[(Int,String,Char,String)](new BroadcastProcessFunction[(Int,String,Int,String),
    (Int,Char),(Int,String,Char,String)
    ] {
      override def processElement(in1: (Int, String, Int, String), readOnlyContext: BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)]#ReadOnlyContext, collector: Collector[(Int, String, Char, String)]): Unit = {
        val gender=in1._3
        val gen=readOnlyContext.getBroadcastState(broadcastInfo).get(gender)
        collector.collect(in1._1,in1._2,gen,in1._4)
      }

      override def processBroadcastElement(in2: (Int, Char), context: BroadcastProcessFunction[(Int, String, Int, String), (Int, Char), (Int, String, Char, String)]#Context, collector: Collector[(Int, String, Char, String)]): Unit = {
         val gen:Int=in2._1
        val gen2:Char=in2._2
        context.getBroadcastState(broadcastInfo).put(gen,gen2)
      }
    }).print()

env.execute()
  }
}
