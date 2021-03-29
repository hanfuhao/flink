package sink


import java.lang
import java.nio.charset.StandardCharsets
import java.util.Properties

import day3.Raytek
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema
import org.apache.kafka.clients.producer.ProducerRecord


object Kafka2Kafka2 {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val producerTopic="raytekTarget"
    val props=new Properties

    props.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val Stream=env.addSource(new FlinkKafkaConsumer[String]("raytekSrc",new SimpleStringSchema,props))
      .filter(x=>x.nonEmpty)
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


val serializationSchema: KafkaSerializationSchema[String] = new KafkaSerializationSchema[String]() {
  override def serialize(t: String, aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
    new ProducerRecord[Array[Byte], Array[Byte]](producerTopic, t.getBytes(StandardCharsets.UTF_8))
  }
}
    val producerConfig = new Properties()
    producerConfig.load(this.getClass.getClassLoader.getResourceAsStream("producer1.properties"))

    val semantic: FlinkKafkaProducer.Semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE

    Stream.addSink(new FlinkKafkaProducer[String](producerTopic,serializationSchema,producerConfig,semantic))

   env.execute()
  }
}
