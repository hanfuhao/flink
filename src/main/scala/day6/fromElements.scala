package day6

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object fromElements {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //普通数据类型
//    env.fromElements(1,2,3).print()

    //元组数据leix
//    env.fromElements(("路飞",56),("白胡子",34),("香克斯",99))
//      .filter(x=>x._2>60)
//      .print()

    //样例类
//    env.fromElements(Person("路飞",56),Person("白胡子",34),Person("香克斯",99))
//        .filter(x=>x.score>60)
//        .print()

     //POJO(Plain old java object
//    env.fromElements(new Person1("路飞",56.0),new Person1("白胡子",34.1),new Person1("香克斯",99.9))
//    .filter(x=>x.getScore>60)
//    .print()

      // lombok框架
//        env.fromElements(new Person3("路飞",56.0),new Person3("白胡子",34.1),new Person3("香克斯",99.9))
//        .filter(x=>x.getScore>60)
//        .print()

    env.execute()
  }
}
