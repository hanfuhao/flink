package day7

import day3.Raytek
import org.apache.flink.api.common.accumulators.IntCounter
import org.apache.flink.api.common.functions.{RichMapFunction, RuntimeContext}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object Accumulator {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    if(args.length!=4 ||args==null){
      sys.error("<!--警告，请输入正确的端口号>")
      sys.exit(-1)
    }
    val tool=ParameterTool.fromArgs(args)
    val hostname=tool.get("hostname")
    val port=tool.getInt("port")

    import org.apache.flink.api.scala._
    env.socketTextStream(hostname,port)
        //.filter(x=>x.trim.nonEmpty)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      }).map(new RichMapFunction[Raytek,(Raytek,String)] {
//只执行一次
      var total:IntCounter=_
      var normal:IntCounter=_
      var execption:IntCounter=_

      override def open(parameters: Configuration): Unit = {


    //初始化累加器
       total=new IntCounter()
       normal=new IntCounter()
        execption=new IntCounter()
     //注册累加器
        val contear:RuntimeContext=getRuntimeContext
        contear.addAccumulator("total",total)
        contear.addAccumulator("normal",normal)
        contear.addAccumulator("execption",execption)


      }
 //每来一条数据，就执行一次
      override def map(in: Raytek): (Raytek, String) = {
        total.add(1)
     if(in.temperature>36.3&&in.temperature<37.2){
            normal.add(1)
       (in,"您的体温正常，祝你旅途愉快")
     }else{
       execption.add(1)
       (in,s"抱歉你的体温是${in.temperature},你即将被隔离")
     }

      }
//数据的关闭
      override def close(): Unit = {
      }
    }).print()
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    //getAccumulatorResult[Int],泛型一定要写，不然报错
    //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
       val result=  env.execute()
       val total=result.getAccumulatorResult[Int]("total")
       val normal=result.getAccumulatorResult[Int]("normal")
       val execption=result.getAccumulatorResult[Int]("execption")
       println(total,normal,execption)

  }
}
