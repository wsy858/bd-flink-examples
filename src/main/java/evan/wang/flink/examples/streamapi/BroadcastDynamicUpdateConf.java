package evan.wang.flink.examples.streamapi;


import com.alibaba.fastjson.JSONObject;
import evan.wang.flink.examples.common.JsonObjectDeserializationSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

/**
 * 使用广播变量动态更新配置
 */
public class BroadcastDynamicUpdateConf {
    static Logger logger = LoggerFactory.getLogger(BroadcastDynamicUpdateConf.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //构造一个map state descriptor
        MapStateDescriptor<String, Long> confDescriptor = new MapStateDescriptor<>(
                "config-keywords",
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.LONG_TYPE_INFO);
        //配置流，比如我们可以从kafka动态接受配置，或者循环去读取数据库之类的
        DataStream<Integer> confStream = env.addSource(new BroadcastSource()).name("广播数据源").setParallelism(1);
        //构造广播流
        BroadcastStream<Integer> broadcastStream = confStream.broadcast(confDescriptor);

        //构造一个业务数据流, 发送{"product": "p1", "num": 21}
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "x.x.x.x:9092"); //todo 更改kafka配置
        properties.setProperty("group.id", "test1");
        FlinkKafkaConsumer<JSONObject> source = new FlinkKafkaConsumer<>("flink_broadcast_test1", new JsonObjectDeserializationSchema(), properties);
        SingleOutputStreamOperator<JSONObject> sourceStream = env.addSource(source).name("业务数据源").setParallelism(1);

        SingleOutputStreamOperator<String> outStream = sourceStream.connect(broadcastStream).process(new BroadcastProcessFunction<JSONObject, Integer, String>() {
            @Override
            public void processElement(JSONObject value, ReadOnlyContext ctx, Collector<String> out) throws Exception {
                // 获取广播变量的值
                Long v = ctx.getBroadcastState(confDescriptor).get("value");
                //logger.info("$$$$$$$$$$$$$value = [" + value + "], v = [" + v + "]");
                if (v != null && value.getLong("num") > v) {
                    String message = String.format("收到了一个大于阈值%s的结果%s", v, value.getLong("num"));
                    out.collect(message);
                }
            }

            @Override
            public void processBroadcastElement(Integer value, Context ctx, Collector<String> out) throws Exception {
                //logger.info("%%%%%%%%%%%%%value = [" + value + "]");
                //更新广播变量的值
                ctx.getBroadcastState(confDescriptor).put("value", value.longValue());
            }
        });

        outStream.print().setParallelism(1);

        env.execute("BroadcastDynamicUpdate");

    }


    public static class BroadcastSource implements SourceFunction<Integer> {

        volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (isRunning) {
                //模拟配置数据
                int configValue = randInt(15, 20);
                //实际可查询数据库获取配置数据......
                ctx.collect(configValue);
                Thread.sleep(10 * 1000);
            }
        }

        /**
         * 生成指定范围内的随机数
         */
        private int randInt(int min, int max) {
            return new Random().nextInt((max - min) + 1) + min;
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }


}
