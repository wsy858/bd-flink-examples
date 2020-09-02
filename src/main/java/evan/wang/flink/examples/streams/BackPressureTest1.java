package evan.wang.flink.examples.streams;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * 反压测试
 * 1、自定义source，快速每隔1毫秒生产1条数据
 * 2、自定义sink，消费数据较慢（耗时2秒）
 * 启动参数：-Xms75M -Xmx75M
 */
public class BackPressureTest1 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Item> source = env.addSource(new MyStreamingSource(), "source-custom-num1")
                .setParallelism(1);
        SingleOutputStreamOperator<String> ss = source.map((MapFunction<Item, String>) Item::getName);
        //打印结果
        ss.addSink(new SinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                System.out.println(value);
                Thread.sleep(2 * 1000);
            }
        }).setParallelism(1);
        String jobName = "user defined streaming source";
        env.execute(jobName);
    }



    private static class MyStreamingSource implements SourceFunction<Item> {

        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<Item> ctx) throws Exception {
            while (isRunning) {
                Item item = generateItem();
                ctx.collect(item);
                //每秒产生一条数据
               // Thread.sleep(1);
                TimeUnit.NANOSECONDS.sleep(1);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }

        //随机产生一条商品数据
        private Item generateItem() {
            int i = new Random().nextInt(100);
            Item item = new Item();
            item.setCreateTime(System.currentTimeMillis());
            item.setName("name" + i);
            item.setId(i);
            return item;
        }
    }


    private static class Item {
        private String name;
        private Integer id;
        private long createTime;

        public Item(String name, Integer id, long createTime) {
            this.name = name;
            this.id = id;
            this.createTime = createTime;
        }

        public Item() {
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getId() {
            return id;
        }

        public void setId(Integer id) {
            this.id = id;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    ", createTime=" + createTime +
                    '}';
        }
    }

}


