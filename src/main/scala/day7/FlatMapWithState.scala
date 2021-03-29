package day7

import day3.Raytek
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FlatMapWithState {
  def main(args: Array[String]): Unit = {

    val env= {
      StreamExecutionEnvironment.getExecutionEnvironment
    }
    import org.apache.flink.api.scala._
    env.socketTextStream("Hadoop2",7777)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      }).keyBy("name")
     .flatMapWithState[(Raytek,String),Double]{
       case(tra,None)=>{
         val now=tra.temperature
         val normal=now>=36.2 &&now<=37.2
         if(normal){
           (List.empty,Some(now))
         }else{
           (List((tra,"你体温异常")),Some(now))
         }


       }
         case(tra,last)=>{
           val lasttem=last.get
           val now=tra.temperature
           val normal=now>=36.2 &&now<=37.2
           if(normal){
             val mid=(lasttem-now).abs
             if(mid>0.5){
               (List((tra,"存在体温差，即将隔离")),Some(now))
             }else{
               (List((tra,"存在体温差，即将隔离")),Some(now))
             }

           }else{
             (List((tra,"你体温异常")),Some(now))
           }
         }
     }.print()


env.execute()


  }
}
