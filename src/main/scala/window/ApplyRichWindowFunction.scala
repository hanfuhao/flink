package window

import java.text.SimpleDateFormat

import day3.Raytek
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.RichWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ApplyRichWindowFunction {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    env.socketTextStream("Hadoop2",9999)
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
      .apply(new RichWindowFunction[Raytek,String,Tuple,TimeWindow] {
        val timeFormwt=new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
        override def apply(key: Tuple, window: TimeWindow, input: Iterable[Raytek], out: Collector[String]): Unit = {
          val keys1=key.getField[String](0)
          var total=0.0
          input.foreach(x=>{
            total=total+x.temperature
          })
          val avg=total/input.size
          val start=timeFormwt.format(window.getStart)
          val end=timeFormwt.format(window.getEnd)
          val result=f"$keys1,$avg%.4f,$start,$end"
          out.collect(result)
        }
      }).print()
    env.execute()
  }
}
