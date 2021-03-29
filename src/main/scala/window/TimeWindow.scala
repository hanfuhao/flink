package window

import day3.Raytek
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object TimeWindow {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    if(args.length!=4||args==null){
      sys.error("请输入hostanme,port")
      sys.exit(-1)
    }
    val tool=ParameterTool.fromArgs(args)
    val hostname=tool.get("hostname")
    val port=tool.getInt("port")
    import org.apache.flink.api.scala._
   val stream= env.socketTextStream(hostname,port)
      .filter(x=>x.trim.nonEmpty)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      })
//stream.keyBy("id")
//  .timeWindow(Time.seconds(5))
//  .reduce((x,y)=>{
//    if(x.temperature>y.temperature){
//      x
//    }else{
//      y
//    }
//  }).print()

       stream.keyBy("id")
      .timeWindow(Time.seconds(5),Time.seconds(2))
         .reduce((x,y)=>{
           if(x.temperature>y.temperature){
             x
           }else{
             y
           }
         }).print()

    env.execute(this.getClass.getSimpleName)
  }
}
