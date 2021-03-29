package day5

import day3.Raytek
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object process {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //对侧输出数据进行标识
    val outputTag=new OutputTag[Raytek]("异常")

    val stream= env.socketTextStream("Hadoop2",9999)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
        //ProcessFunction，一个函数类，来处理流中的元素
        //new ProcessFunction[Raytek,Raytek]，[Raytek,Raytek]，一个输入，一个输出
        //ProcessFunction注意是一个抽象类，原则上不能new ，但是可以用内部类New
      }).process(new ProcessFunction[Raytek,Raytek]() {
      //value: Raytek传入的数据
      //#Context，一个内部类
      override def processElement(value: Raytek, context: ProcessFunction[Raytek, Raytek]#Context,
                                       out: Collector[Raytek]): Unit = {
          if(value.temperature>=36.3&&value.temperature<37.2){
            //输出主输出流
            out.collect(value)
          }else{
            //输出侧输出流
            context.output(outputTag,value)
          }
      }
    })
      stream.getSideOutput(outputTag).print("该旅客体温异常")
      stream.print("祝你旅途愉快")

    env.execute(this.getClass.getSimpleName)
  }
}
