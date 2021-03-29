package window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ReducingState, ReducingStateDescriptor}
import org.apache.flink.api.common.typeutils.base.LongSerializer
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{CountTrigger, Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object SelfTriggerDemo {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment


    import org.apache.flink.api.scala._

    env.socketTextStream("Hadoop2", 6666)
      .filter(perEle => perEle.trim.nonEmpty && perEle.trim.contains(","))
      .map(ferryBus => { //ferryBus中封装的是一个旅客信息，信息中包含两个字段：摆渡车的编号,旅客的姓名  ，如：fb_1,jack
        val info = ferryBus.split(",")
        val fbNo = info(0).trim
        val name = info(1).trim
        (fbNo, 1)
      }).keyBy(0)
      .timeWindow(Time.seconds(5))
      .trigger(new MyTrigger(4))
      .sum(1)
      .print("自定义触发器执行效果 → ")

    //④启动
    env.execute
  }

  /**
    * 自定义触发器子类
    */
  class MyTrigger(maxCount: Long) extends Trigger[(String, Int), TimeWindow] {

    private val stateDesc = new ReducingStateDescriptor[Long]("count", new Sum, classOf[Long])

    private var cnt = this.maxCount


    override def onElement(element: (String, Int), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      ctx.registerProcessingTimeTimer(window.maxTimestamp)


      val count = ctx.getPartitionedState(stateDesc)
      count.add(1L)
      if (count.get >= maxCount) {
        count.clear()
        println("旅客坐满了，要发车了....")
        return TriggerResult.FIRE_AND_PURGE
      }

      // 让旅客继续上车
      return TriggerResult.CONTINUE
    }

    /**
      * 到了处理的时间了，窗口关闭，触发执行
      *
      * @param time
      * @param window
      * @param ctx
      * @return
      */
    override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
      println("时间到点了，该发车了。。。。")
      TriggerResult.FIRE_AND_PURGE
    }

    /**
      * 事件时间到了，窗口关闭，触发执行
      *
      * @param time
      * @param window
      * @param ctx
      * @return
      */
    override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

    /**
      * 窗口关闭时，窗口中的元素要清空
      *
      * @param window
      * @param ctx
      */
    override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {
      ctx.deleteProcessingTimeTimer(window.maxTimestamp())
      ctx.getPartitionedState(stateDesc).clear()
    }

    override def canMerge = true


    @throws[Exception]
    override def onMerge(window: TimeWindow, ctx: Trigger.OnMergeContext): Unit = {
      ctx.mergePartitionedState(stateDesc)
    }

    @SerialVersionUID(1L)
    private class Sum extends ReduceFunction[Long] {
      @throws[Exception]
      override def reduce(value1: Long, value2: Long): Long = value1 + value2
    }

  }

}
