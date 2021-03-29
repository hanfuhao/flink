package sink

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.util.Collector


object UnbondJoin {
  def main(args: Array[String]): Unit = {
    val env=StreamExecutionEnvironment.getExecutionEnvironment
    val str= env.fromElements((1,'男'),(2,'女'))
    val str2=env.fromElements(("liming",19,1),("yibo",26,2))
env.setParallelism(3)

    //将存储性别信息的无界流封装成广播流
    //广播流的封装类型需要定义
    //元素一：广播流的整体元素的名称，
    // 元素二：→广播流中每个元素的key对应的类型信息，需要对应的类型是TypeInformation， BasicTypeInfo继承自TypeInformation
    //→广播流中每个元素的value对应的类型信息，需要对应的类型是TypeInformation， BasicTypeInfo继承自TypeInformation
 val desc=new MapStateDescriptor("brodecast",BasicTypeInfo.INT_TYPE_INFO,BasicTypeInfo.CHAR_TYPE_INFO)
    val destream=str.broadcast(desc)
    //使用connect将广播流和无界流连接起来,并使用自定义的广播方法对数据进行处理
    str2.connect(destream).process(new MyBrodcast(desc)).print()

    env.execute(this.getClass.getSimpleName)
  }
}
class MyBrodcast(descr: MapStateDescriptor[Integer, Character]) extends BroadcastProcessFunction[(String,Int,Int),(Int,Char),(String,Int,Char)] {
  override def processElement(value: (String, Int, Int), ctx: BroadcastProcessFunction[(String, Int, Int), (Int, Char),
    (String, Int, Char)]#ReadOnlyContext, out: Collector[(String, Int, Char)]): Unit = {
//    //获取性别
//    val flag=value._3
//   val sex= ctx.getBroadcastState(descr).get(flag)
//   out.collect((value._1,value._2,sex))

    //获得当前学生信息中的性别flg
    val genderFlg = value._3

    //从广播流中获得数据
    val genderName = ctx.getBroadcastState(descr).get(genderFlg)

    //将当前处理后的学生信息送往目标DataStream
    out.collect((value._1, value._2, genderName))
  }

  override def processBroadcastElement(value: (Int, Char), ctx: BroadcastProcessFunction[(String, Int, Int), (Int, Char),
    (String, Int, Char)]#Context, out: Collector[(String, Int, Char)]): Unit = {
    val genderFlg: Int = value._1
    val genderName: Char = value._2
    ctx.getBroadcastState(descr).put(genderFlg, genderName)

  }
}