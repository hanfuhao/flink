package BroadCast

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object UnboundBroadcast {
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._

    val ds1=env.fromElements((1,'男'),(2,'女'))
    val ds2=env.fromElements((101,1,"李煜","广东"),(102,2,"冰冰","北京"))

ds2.map(new RichMapFunction[(Int,Int,String,String),(Int,Char,String,String)] {
  import scala.collection.mutable

val container:mutable.Map[Int,Char]=mutable.Map[Int,Char]()

  override def open(parameters: Configuration): Unit = {
    val lst: java.util.List[(Int, Char)] = getRuntimeContext().getBroadcastVariable("broadcast")

    import scala.collection.JavaConversions._
    for(x<- lst){
      container.put(x._1,x._2)
    }

  }

  override def map(in: (Int, Int, String, String)): (Int, Char, String, String) ={
   val gender=in._2
    val value = container.getOrElse(gender, 'x')
    val gen=value
    (in._1,gen,in._3,in._4)
  }
}).withBroadcastSet(ds1,"broadcast")
      .print()



  }
}
