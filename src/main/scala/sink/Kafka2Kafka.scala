package sink

import java.util.Properties

import day3.Raytek
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer}

object Kafka2Kafka {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
  val props=new Properties
    props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
val stream=env.addSource(new FlinkKafkaConsumer[String]("raytekSrc",new SimpleStringSchema,props))
  .map(x=>{
    val str= x.split(",")
    val id= str(0).trim()
    val temperature=str(1).trim().toDouble
    val name=str(2).trim()
    val time=str(3).trim().toLong
    val location=str(4).trim()
    Raytek(id,temperature,name,time,location)
  }).filter(x=>{
 val tem= x.temperature
  val normal=(tem>=36.3&&tem<37.2)
  !normal
}).map(x=>x.toString)
//map(x=>x.toString),一定是string,否则下面报错
  stream.addSink(new FlinkKafkaProducer[String]("192.168.253.104:9092,192.168.253.105:9092,192.168.253.106:9092",
  "raytekTarget",new SimpleStringSchema()
  ))

    env.execute()
  }
}
