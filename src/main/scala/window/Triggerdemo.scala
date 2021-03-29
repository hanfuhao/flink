package window


import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow


object Triggerdemo {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    import org.apache.flink.api.scala._
    val stream= env.socketTextStream("Hadoop2",8888)
      .map(x=>{
        (x,1.0)
      }).keyBy(0)
      .timeWindow(Time.seconds(3))
      .trigger(new Trigger[(String,Double),TimeWindow] {
        var cnt=0
        override def onElement(t: (String, Double), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
          if(cnt>=4){
            cnt=0
            print("触发TRIGGER，即将发车")
            TriggerResult.FIRE
          }else{
            cnt=cnt+1;
            TriggerResult.CONTINUE
          }
        }

        override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
          print("时间到点了")
          TriggerResult.FIRE
        }

        override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
          TriggerResult.CONTINUE
        }

        override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {
         triggerContext.deleteProcessingTimeTimer(w.maxTimestamp())
        }
      }).sum(1)
      .print()

env.execute()
  }
}
