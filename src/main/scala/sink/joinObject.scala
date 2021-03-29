package sink

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

import scala.collection.mutable

object joinObject {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val str= env.fromElements((1,'男'),(2,'女'))
    val str2=env.fromElements(("liming",19,1),("yibo",26,2))
    str2.map(new RichMapFunction[(String,Int,Int),(String,Int,Char)] {
      import scala.collection.mutable._
      //
      var container: Map[Int, Char] = _
//      override def open(parameters: Configuration): Unit = {
////      val brt: java.util.List[(Int, Char)]=  getRuntimeContext().getBroadcastVariable[(Int,Char)]("brodcast")
////        import scala.collection.JavaConversions._
////        for(x<- brt){
////          container.put(x._1,x._2)
////        }
////      }

      override def open(parameters: Configuration): Unit = {
        //初始化全局变量container
      container = mutable.Map()
        //步骤：
        //a)获得广播变量中封装的性别信息
        val lst: java.util.List[(Int, Char)] = getRuntimeContext().getBroadcastVariable("brodcast")
        //b)将信息拿出来，存入到Map集合中
        import scala.collection.JavaConversions._
        for (perEle <- lst) {
          container.put(perEle._1, perEle._2)
        }

      }

      override def map(value: (String, Int, Int)): (String, Int, Char) = {
        val sex=value._3
        var c: Char = container.getOrElse(sex, 'x')
        val tmp= c
        (value._1,value._2,tmp)
      }
    }).withBroadcastSet(str,"brodcast")
      .print()

  }
}
