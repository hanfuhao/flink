package day7

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FsStateBackend {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    //此时横线并不是代表过时，只是不建议硬编码
    //设置chackpoint的存储位置
     env.setStateBackend(new FsStateBackend("hdfs://Hadoop2/flink/state/fs"))
    //必须开启，10秒进行一次Checkpointing
     env.enableCheckpointing(10000)

    env.socketTextStream("Hadoop2",7777)
      .filter(x=>x.nonEmpty)
      .flatMap(x=>{
        x.split("\\s+")
      }).map(x=>{
      (x,1)
    }).keyBy(0)
      .sum(1)
      .print()

    env.execute(this.getClass.getSimpleName)
      //设置重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(60,Time.of(10,TimeUnit.SECONDS)))
  }
}
