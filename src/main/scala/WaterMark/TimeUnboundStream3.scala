package WaterMark

import java.text.SimpleDateFormat

import WaterMark.TimeUnboundStream2.MyAssignerWithPeriodicWatermarks
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TimeUnboundStream3 {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val outputag=new OutputTag[(String,Long)]("slide_stream")

    val stream=env.socketTextStream("Hadoop2",6666)
      .filter(x=>x.trim.nonEmpty)
      .map(perTraveller => {
        val arr = perTraveller.split(",")
        val raytekId = arr(0).trim
        val ts = arr(1).trim.toLong
        (raytekId, ts)
      }).assignTimestampsAndWatermarks(new MyAssignerWithPeriodicWatermarks)
      .keyBy(0)
      .timeWindow(Time.seconds(3))
      .sideOutputLateData(outputag)
      .apply(new RichWindowFunction[(String,Long),String,Tuple,TimeWindow] {
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[(String, Long)], out: Collector[String]): Unit = {

          val sdf=new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒SSS毫秒")
          val itr = input.toList.sortBy(_._2)
          val firstTravellerTime = itr.head._2
          val lastTravellerTime = itr.last._2
          //②获得窗口的开始时间和结束时间
          val beginTime = window.getStart
          val endTime = window.getEnd
          //③返回结果
          val result = s"红外测温仪的id是→ ${key.getField(0)} | 当前窗口中最先抵达的旅客对应的时间是→$firstTravellerTime，时间格式化之后→ ${sdf.format(firstTravellerTime)} | 以及最后抵达的旅客对应的时间是→$lastTravellerTime，时间格式化之后→ ${sdf.format(lastTravellerTime)} | 窗口开始的时间→$beginTime,时间格式化之后→${sdf.format(beginTime)} | 窗口结束的时间→$endTime ,时间格式化之后→${sdf.format(endTime)}"
          out.collect(result)
        }
      })
          stream.print("正常的输出流-》")
         stream.getSideOutput(outputag).print("异常流-》")
    env.execute()

  }
  class MyAssignerWithPeriodicWatermarks extends AssignerWithPeriodicWatermarks[(String, Long)] {
    private var maxTimestamp = 0L
    private var maxOrderedTime = 10000L
    private val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy年MM月dd日 HH时mm分ss秒SSS毫秒")

    override def getCurrentWatermark: Watermark = new Watermark(maxTimestamp - maxOrderedTime)

    override def extractTimestamp(element: (String, Long), previousElementTimestamp: Long): Long = {
      val currentTimestamp = element._2
      maxTimestamp = maxTimestamp.max(currentTimestamp)
      val wmTimestamp = getCurrentWatermark.getTimestamp
      val result = s"当前记录信息是：EventTime→$currentTimestamp，格式化之后的时间→${sdf.format(currentTimestamp)} | 当前窗口中最大的EventTime→$maxTimestamp，格式化之后的时间→${sdf.format(maxTimestamp)} | 当前的watermark→$wmTimestamp，格式化之后的时间→${sdf.format(wmTimestamp)}"
      println(result)

      //c)返回当前数据真正的时间戳
      currentTimestamp
    }
  }
}
