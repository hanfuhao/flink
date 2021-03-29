package sink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import day3.Raytek
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object JDBC {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    var topic="raytekSrc"
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

Stream.addSink(new MyselfRichSinkFunction)

      env.execute()
  }

  class MyselfRichSinkFunction extends RichSinkFunction[Raytek] {
    /**
     * 连接
     */
    var conn: Connection = _
    /**
     * 进行更新的PreparedStatement
     */
    var updateStatement: PreparedStatement = _

    /**
     * 进行插入的PreparedStatement
     */
    var insertStatement: PreparedStatement = _

    /**
     * 针对于一个DataStream，下述的方法只会执行一次，用来进行初始化的动作
     *
     * @param parameters
     */
    override def open(parameters: Configuration): Unit = {
      conn = DriverManager.getConnection("jdbc:mysql://Hadoop2:3306/flink?serverTimezone=UTC&characterEncoding=utf-8", "root", "123456")

      updateStatement = conn.prepareStatement(
        """
          |update tb_raytek_result
          |set temperature=?,`timestamp`=?,location=?
          |where id=? and `name`=?
        """.stripMargin)

      insertStatement = conn.prepareStatement(
        """
          |insert into  tb_raytek_result(id,temperature,`name`,`timestamp` ,location)
          |values(?,?,?,?,?)
        """.stripMargin)

    }

    /**
     * 每次处理DataStream中的一个元素，下述的方法就执行一次
     *
     * @param value   当前待处理的元素
     * @param context 上下文信息
     */
    override def invoke(value: Raytek, context: SinkFunction.Context[_]): Unit = {
      //步骤：
      //a）进行更新
      updateStatement.setDouble(1, value.temperature)
      updateStatement.setLong(2, value.time)
      updateStatement.setString(3, value.location)
      updateStatement.setString(4, value.id)
      updateStatement.setString(5, value.name)
      updateStatement.executeUpdate()

      //b)若更新失败，进行插入
      if (updateStatement.getUpdateCount == 0) {
        insertStatement.setString(1, value.id)
        insertStatement.setDouble(2, value.temperature)
        insertStatement.setString(3, value.name)
        insertStatement.setLong(4, value.time)
        insertStatement.setString(5, value.location)
        insertStatement.executeUpdate()
      }

    }

    /**
     * 针对于一个DataStream，下述的方法只会执行一次，用来进行资源的释放
     */
    override def close(): Unit = {
      if (updateStatement != null)
        updateStatement.close()

      if (insertStatement != null)
        insertStatement.close()

      if (conn != null)
        conn.close()
    }
  }

}
