package sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import day3.Raytek
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object YHjdbc {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    if(args.length!=4||args==null){
      sys.error("\"<警告，请输入hostname,port>\"")
    sys.exit(-1)
    }

   val tool= ParameterTool.fromArgs(args)
    val host=tool.get("hostname")
    val port=tool.getInt("port")

  val Stream=  env.socketTextStream(host,port)
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


    Stream.addSink(new MyRichFunction)

    env.execute()

  }
  class MyRichFunction extends RichSinkFunction[Raytek]{

    var coon:Connection=_
    var insertOrUpdateStatement:PreparedStatement=_

    override def open(parameters: Configuration): Unit ={

      coon=DriverManager.getConnection("jdbc:mysql://Hadoop2:3306/flink?serverTimezone=UTC&characterEncoding=utf-8", "root", "123456")

      insertOrUpdateStatement = coon.prepareStatement( """
                               |insert into  tb_raytek_result(id,temperature,`name`,`timestamp` ,location)
                               |values(?,?,?,?,?)
                               |on duplicate key update temperature=?,`timestamp`=?,location=?
                              |""".stripMargin)


    }

    override def invoke(value: Raytek, context: SinkFunction.Context[_]): Unit ={
      insertOrUpdateStatement.setString(1, value.id)
      insertOrUpdateStatement.setDouble(2, value.temperature)
      insertOrUpdateStatement.setString(3, value.name)
      insertOrUpdateStatement.setLong(4, value.time)
      insertOrUpdateStatement.setString(5, value.location)

      //给用于更新的占位符赋值，注意：序号从1开始连续计数
      insertOrUpdateStatement.setDouble(6, value.temperature)
      insertOrUpdateStatement.setLong(7, value.time)
      insertOrUpdateStatement.setString(8, value.location)


      //执行（将sql语句传给远程的db server，db server会自动进行判断，若存在就更行，否则，执行插入）
      insertOrUpdateStatement.executeUpdate()

    }

    override def close(): Unit = {
if(insertOrUpdateStatement!=null){
  insertOrUpdateStatement.close()
}
      if(coon!=null){
        coon.close()
      }
    }
  }


}
