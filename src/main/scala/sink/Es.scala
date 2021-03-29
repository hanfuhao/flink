package sink

import java.util.Properties

import day3.Raytek
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

import scala.collection.JavaConversions._

object Es {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //String topic, DeserializationSchema<T> valueDeserializer, Properties props
    var topic="raytekSrc"
    val producerTopic="raytekTarget"
    val properties=new Properties()
    properties.load(this.getClass.getClassLoader.getResourceAsStream("consumer.properties"))
    val Stream=   env.addSource(new FlinkKafkaConsumer[String](topic,new SimpleStringSchema(),properties))
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
    })
   //List<HttpHost> httpHosts, ElasticsearchSinkFunction<T> elasticsearchSinkFunction
    val scalaList=List(
      new HttpHost("Hadoop3",9200),
      new HttpHost("Hadoop4",9200),
      new HttpHost("Hadoop5",9200)
    )
val es=new ElasticsearchSink.Builder[Raytek](scalaList,new MyElasticsearchSinkFunction)

  es.setBulkFlushMaxActions(1)

    Stream.addSink(es.build())

      env.execute(this.getClass.getSimpleName)

  }
  class  MyElasticsearchSinkFunction extends ElasticsearchSinkFunction[Raytek] {
    override def process(t: Raytek, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
      //将当前的信息封装到Map中
      val scalaMap=Map[String,String](
        "id"->t.id.trim,
        "temperature"->t.temperature.toString.trim,
        "name"->t.name,
        "timestamp"->t.time.toString,
        "location"->t.location
      )
      val javaMap:java.util.Map[String,String]=scalaMap
      //构建实例
          Requests.indexRequest()
        .index("raytek")
        .`type`("traceller")
        .id(s"${t.id.trim}->${t.name.trim}")
        .source(javaMap)



    }
  }
}
