package day3

import com.alibaba.fastjson.JSON
import entity.ActionEnum
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.hadoop.hdfs.web.JsonUtil
import org.slf4j.LoggerFactory

import scala.io.Source


object Text {
  def main(args: Array[String]): Unit = {
    get()

  //  print(ActionEnum.PAGE_ENTER_H5.getRemark)

//   val log= LoggerFactory.getLogger("Text")
//
//    try {
//        val s:String="abc"
//      val ss=  Integer.parseInt(s)
//      print("---------ss--------------")
//    }catch {
//      case ex:Exception=>{
//        println(s"FlinkHelper create flink context occur exception：msg=$ex")
//       log.error(ex.getMessage)
//
//      }
//    }


//   env.enableCheckpointing(1000)
//    val exts=
//      """
//        |{"travel_member_adult":"2","userRegion":"222400"}
//        |""".stripMargin
//
//
//  val str=  JSON.parseObject(exts)
//      .getOrDefault("travel_member_adult1", "meyyou").toString
//print(str)


//    env.execute(this.getClass.getSimpleName)
//               val st=Array(1,2,3,"hh").toList
//   for (x<- st){
//    println(x)
//   }


  }
  def get(): Unit ={

  }
}
