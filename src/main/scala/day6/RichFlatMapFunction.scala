package day6

import java.util.Properties

import day3.Raytek
import day6.MyselfTransformation2.id
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object RichFlatMapFunction {
  var topic=""
  val properties=new Properties
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    if(args==null ||args.length!=4){
      sys.error(
        """
          |<--!请输入hostanme,port ,id>
          |""".stripMargin)
      sys.exit(-1)
    }
    val tool=ParameterTool.fromArgs(args)
    val hostname=tool.get("hostname")
    val port=tool.getInt("port")

    topic="tempertureException"

    properties.load(this.getClass.getClassLoader.getResourceAsStream("producer.properties"))
    val stream= env.socketTextStream(hostname,port)
      .map(x=>{
        val str= x.split(",")
        val id= str(0).trim()
        val temperature=str(1).trim().toDouble
        val name=str(2).trim()
        val time=str(3).trim().toLong
        val location=str(4).trim()
        Raytek(id,temperature,name,time,location)
      }).flatMap(new MyRichTransformation)
        .print()

    env.execute()
  }

class MyRichTransformation extends RichFlatMapFunction[Raytek,Raytek]{
  var producer:KafkaProducer[String,String]=_
  override def open(parameters: Configuration): Unit = {
    producer=new KafkaProducer[String,String](properties)
  }


  override def flatMap(in: Raytek, collector: Collector[Raytek]): Unit = {
    val normal:Boolean=in.temperature>=36.3&&in.temperature<=37.2
    if(normal){
      collector.collect(in)
    }else{
      //:ProducerRecord[String,String],一定要写出来，否则报错
      val msg:ProducerRecord[String,String]=new ProducerRecord(topic,in.toString)
      producer.send(msg)
    }
  }


  override def close(){
    if(producer!=null)
      producer.close()
  }
}

}
