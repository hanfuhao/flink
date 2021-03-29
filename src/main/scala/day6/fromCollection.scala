package day6

import java.util
import java.util.Collections

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object fromCollection {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val container=new java.util.LinkedList[Person1]()
    Collections.addAll(
      container,
      new Person1("路飞",56.0),
      new Person1("白胡子",34.1),
      new Person1("香克斯",99.9)
    )
    //导入JavaConversions单例类中的隐式方法，自动将java中的集合与scala中的集合根据需求， 自动进行转换
    import scala.collection.JavaConversions._
    env.fromCollection(container)
      .print()

    env.execute()
  }
}
