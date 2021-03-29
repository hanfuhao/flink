package day7

import java.io.File

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import scala.collection.mutable._
import scala.io.{BufferedSource, Source}
import scala.collection
object registerCachedFile {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment

    //读取数据，放入分布式缓存中，最终由taskManager读取
    env.registerCachedFile("E:\\IdeaProject\\flink\\src\\main\\Log\\a.txt"
      ,"cacheFile")

    import org.apache.flink.api.scala._
    env.socketTextStream("Hadoop2",8888)
      .map(new RichMapFunction[String,(Int,String,Char,String)] {
        val map=Map[Int,Char]()
        var ss:BufferedSource= _

        override def open(parameters: Configuration): Unit = {
          //获取文件
          var file:File= getRuntimeContext.getDistributedCache.getFile("cacheFile")
          //
          ss= Source.fromFile(file)
        val list= ss.getLines().toList
          for(x<-list){
            val str=x.split(",")
            map.put(str(0).toInt,str(1).toCharArray()(0))
          }
        }

        override def map(in: String): (Int, String, Char, String) = {
      val str=in.split(",")
         val gender= map.getOrElse(str(2).toInt,'x')
          (str(0).toInt,str(1).trim,gender,str(3))
        }
        override def close(): Unit ={
          ss.close()
        }

      }).print()
        env.execute()
  }
}
