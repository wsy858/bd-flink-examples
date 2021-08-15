package evan.wang.flink.examples.watermark;

import evan.wang.flink.examples.utils.PropertiesUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/** */
public class WatermarkMain {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 每隔 5s 重启一次，尝试三次如果 Job 还没有起来则停止
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 并行度设置为 1
        env.setParallelism(1);

        //创建自定义水印
        WatermarkStrategy<Word> ws = (ctx -> new WordPeriodicWatermarkNew());
        ws = ws.withTimestampAssigner((word, recordTimestamp) -> word.getTimestamp())
                .withIdleness(Duration.ofSeconds(10)); //数据源空闲多久后，不发出wm

        //build-in 固定延迟水印
        WatermarkStrategy<Word> ws1 = WatermarkStrategy.<Word>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((word, recordTimestamp) -> word.getTimestamp())
                .withIdleness(Duration.ofSeconds(10)); //数据源空闲多久后，不发出wm

        SingleOutputStreamOperator<Word> data = env.socketTextStream(PropertiesUtil.getStrValue("socket.host"),
                PropertiesUtil.getIntValue("socket.port", 9091))
                .map((MapFunction<String, Word>) value -> {
                    String[] split = value.trim().split(",");
                    return new Word(split[0], Integer.parseInt(split[1]), Long.parseLong(split[2]));
                }).assignTimestampsAndWatermarks(ws);
                //.assignTimestampsAndWatermarks(new WordPeriodicWatermark()); //已废弃方式

        data.keyBy("word")
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.milliseconds(5))
                .sum("count")
                .print();

        env.execute("watermark demo");
    }
}

/*
nc -lk 9091

a,1,1599113953000    wm 1599113948000
a,1,1599113954000
a,1,1599113955000

a,1,1599113960000
a,1,1599113962000    wm 1599113957000
a,1,1599113964000
a,1,1599113965000    wm 1599113960000    窗口1： 1599113950000 ----- 1599113959000,  触发条件 wm > 窗口结束时间， 统计到的值为count=3
a,1,1599113966000
a,1,1599113967000
a,1,1599113969000


a,1,1599113973000
a,1,1599113974000
a,1,1599113975000    wm 1599113970000    窗口2： 1599113960000 ----- 1599113969000， count=6
a,1,1599113976000

a,1,1599113964000   wm 1599113971000    过时数据
a,1,1599113985000   wm 1599113980000    窗口3： 1599113970000 ----- 1599113979000， count=3
a,1,1599113968000   wm 1599113980000    过时数据


a,1,1599113994000
a,1,1599113995000
a,1,1599113996000
*/
