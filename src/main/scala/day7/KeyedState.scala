package day7

import day3.Raytek
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector


object KeyedState {
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
      }).keyBy("name")
      .flatMap(new RichFlatMapFunction[Raytek,(Raytek,String)] {
               var valuestate:ValueState[Double] =_

        override def open(parameters: Configuration): Unit = {
             //对元素名字类型进行封装
              val  desc=  new ValueStateDescriptor("name",classOf[Double])
             //进行注册
              valuestate= getRuntimeContext.getState(desc)
        }

        override def flatMap(in: Raytek, collector: Collector[(Raytek, String)]): Unit = {
         val last= valuestate.value()
          val now=in.temperature
          val normal=now>36.3&& now<37.3
          if(normal){
            if(last>0){
              //取绝对值
              val mid=(now-last).abs
              if(mid>0.5){
                collector.collect(in,s"异常：你本次的温度是$now,你上次的温度是$last,两次温度差高于0.8，你的体温异常")
                valuestate.update(now)
              }else{
                collector.collect(in,"你的体温正常，祝你旅途愉快")
                valuestate.update(now)
              }
            }else{
              collector.collect(in,"你的体温正常，祝你旅途愉快")
              valuestate.update(now)
            }


          }else{
            collector.collect(in,s"异常，你的体温是$now,你即将被隔离")
            valuestate.update(now)
          }
        }

        override def close(): Unit = {

        }

      }).print()

    env.execute()
  }
}
