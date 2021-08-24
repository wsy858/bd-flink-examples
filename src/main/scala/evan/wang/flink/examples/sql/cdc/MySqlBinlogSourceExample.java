package evan.wang.flink.examples.sql.cdc;

import evan.wang.flink.examples.utils.PropertiesUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.MySqlSource;

public class MySqlBinlogSourceExample {

    public static void main(String[] args) throws Exception {
        String host = PropertiesUtil.getStrValue("mysql.host");
        String port = PropertiesUtil.getStrValue("mysql.port");
        String username = PropertiesUtil.getStrValue("mysql.username");
        String password = PropertiesUtil.getStrValue("mysql.password");
        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname(host)
                .port(port == null ? 3306 : Integer.parseInt(port))
                .databaseList("test") // monitor all tables under inventory database
                .username(username)
                .password(password)
                .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(3000); // checkpoint every 3000 milliseconds
        env.addSource(sourceFunction)
                .print()
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
