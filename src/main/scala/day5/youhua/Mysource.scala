package day5.youhua

import day3.Raytek
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.io.Source

object Mysource extends SourceFunction[Raytek]{
  var cnt = 0
  var flag = true

  override def run(sourceContext: SourceFunction.SourceContext[Raytek]): Unit = {
    val file = Source.fromFile("src/main/Log/raytek.log").getLines().toList
    while (cnt < file.size && flag) {
      val str = file(cnt).split(",")
      val id = str(0).trim()
      val temperature = str(1).trim().toDouble
      val name = str(2).trim()
      val time = str(3).trim().toLong
      val location = str(4).trim()
      val ray = Raytek(id, temperature, name, time, location)
      //将数据发送
      sourceContext.collect(ray)
      cnt = cnt + 1
    }

  }

  override def cancel(): Unit = {
    flag = false
  }
}
