package sink

import java.util.Properties

import day3.Raytek
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisClusterConfig, FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

object redis {
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
    //FlinkJedisConfigBase flinkJedisConfigBase, RedisMapper<IN> redisSinkMapper

    val flinkJedisConfigBase:FlinkJedisConfigBase=new FlinkJedisPoolConfig.Builder()
        .setHost("Hadoop2")
        .setPort(6379)
        .build()

    Stream.addSink(new RedisSink[Raytek](flinkJedisConfigBase,new MyRedisMapper))

    env.execute()

  }

  class MyRedisMapper extends RedisMapper[Raytek]{
    override def getCommandDescription: RedisCommandDescription = {
      //用来指定数据的类型
      new RedisCommandDescription(RedisCommand.HSET,
        "allTempExceptionTravllers"
      )
    }


    //用来指定key
    override def getKeyFromData(t: Raytek): String = {
     t.id+t.name
    }

    override def getValueFromData(t: Raytek): String = {
             t.toString
    }
  }
}
