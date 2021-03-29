package window

import day3.Raytek
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object CountWindow {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream= env.socketTextStream("Hadoop2",9999)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      })

    stream.keyBy("id")
      .countWindow(3)
      .reduce((x,y)=>{
        if(x.temperature>y.temperature){
          x
        }else{
          y
        }
      }).print()


//    stream.keyBy("id")
//      .countWindow(5,2)
//      .reduce((x,y)=>{
//        if(x.temperature>y.temperature){
//          x
//        }else{
//          y
//        }
//      }).print()
env.execute()
  }
}
