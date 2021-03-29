package day5

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object FromKafka {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    //导入隐式转换
    import org.apache.flink.api.scala._
    //定义哪个topic
    val topic="raytek"
    //反序列化方式
    val valueDeserilalizer=new SimpleStringSchema()
       //配置文件
    val props=new Properties()
    //new FlinkKafkaConsumer[String],String是消息的类型
    props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    env.addSource(new FlinkKafkaConsumer[String](topic,valueDeserilalizer,props))
      .print()

//启动
    env.execute(this.getClass.getSimpleName)
  }
}
