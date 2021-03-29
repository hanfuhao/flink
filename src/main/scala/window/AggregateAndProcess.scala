package window

import java.text.SimpleDateFormat

import day3.Raytek
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object AggregateAndProcess {
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
      .aggregate(new MyAggregate,new MyProcess).print()

    env.execute()
  }
  class MyAggregate extends AggregateFunction[Raytek,(String,Int,Double),(String,Double)] {
    override def createAccumulator(): (String, Int, Double) = {
      ("",0,0.0)
    }

    override def add(value: Raytek, accumulator: (String, Int, Double)): (String, Int, Double) = {
      (value.id,accumulator._2+1,accumulator._3+value.temperature)
    }

    override def merge(a: (String, Int, Double), b: (String, Int, Double)): (String, Int, Double) = {
      (a._1,a._2+b._2,a._3+b._3)
    }
    override def getResult(accumulator: (String, Int, Double)): (String, Double) = {
      (accumulator._1,accumulator._3/accumulator._2)
    }
  }
  class MyProcess extends ProcessWindowFunction[(String,Double),String,Tuple,TimeWindow] {
    override def process(key: Tuple, context: Context, elements: Iterable[(String, Double)], out: Collector[String]): Unit ={
      val key1=key.getField[String](0)
      val id=elements.toList(0)._1
      val tem=elements.toList(0)._2
      val win=context.window
      val formatTime=new SimpleDateFormat("yyyy-MM-dd HH-mm-ss")
      val begintime=formatTime.format(win.getStart)
      val endtime=formatTime.format(win.getEnd)
      val result=f"$id----------------,$tem,$begintime,$endtime"
      out.collect(result)
    }
  }
}
