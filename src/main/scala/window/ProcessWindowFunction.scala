package window

//import day3.Raytek
//import org.apache.flink.api.java.tuple.Tuple
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
//import org.apache.flink.streaming.api.windowing.time.Time
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow
//import org.apache.flink.util.Collector


import day3.Raytek
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object ProcessWindowFunction {
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
      //求平均体温
      .process(new ProcessWindowFunction[Raytek,(String,Double),Tuple,TimeWindow] {
        override def process(key: Tuple, context: Context, elements: Iterable[Raytek], out: Collector[(String, Double)]): Unit ={

        val cnt=elements.size
        var total=0.0
        elements.foreach(x=>{
          total=total+x.temperature
        })
        //key.getField[String],要指出泛型，因为nothing是所有类型的字类
        val keys1=key.getField[String](0)
        val result=(keys1,total/cnt)
        //val result=(key.getField(0),total/cnt),可以指出，因为这个方法已经写出了输出类型，会自动匹配
        out.collect(result)
      }

      }).print()

         env.execute(this.getClass.getSimpleName)

  }
}
