package window

import java.text.SimpleDateFormat

import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object WaterMark1 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //将并行度调为一
    env.setParallelism(1)
    //
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.socketTextStream("Hadoop2",8888)
    .map(x=>{
      val str=  x.split(",")
        (str(0),str(1).toLong)
      })
      .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks[(String,Long)] {
       //迄今为止最大的时间戳
        var maxTimestamp=0L
        //最大的延迟时间
        var maxdelayTime=10000L
        val timeFormwt=new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
        override def getCurrentWatermark: Watermark = {
             new Watermark(maxTimestamp-maxdelayTime)
        }

        override def extractTimestamp(t: (String, Long), l: Long): Long = {
          val cuurentime=t._2
          maxTimestamp=maxTimestamp.max(cuurentime)
//        val vm=  getCurrentWatermark.getTimestamp
//          val result=s"$cuurentime"
         cuurentime
        }
      })
      .keyBy(0)
      .timeWindow(Time.seconds(5))
      .apply(new RichWindowFunction[(String,Long),String,Tuple,TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit ={
        val list= input.toList.sortBy(_._2)
          val min=  list.head._2
          val max=list.last._2
          val timeFormwt=new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
          val begin=timeFormwt.format(window.getStart)
          val end=timeFormwt.format(window.getEnd)
          val result=s"${key.getField(0)}window开始时间$begin,结束时间$end,最早时间$min,最晚时间$max"
          out.collect(result)
        }
      }).print()
env.execute()
  }
}
