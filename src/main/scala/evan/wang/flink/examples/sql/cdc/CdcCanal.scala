package evan.wang.flink.examples.sql.cdc

import evan.wang.flink.examples.utils.PropertiesUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table

/**
 * 基于Canal同步MySQL增量数据
 * 流程: (MySQL binlog) ——> (Canal) ——> (Kafka) ——> (Flink) ——> (Sink)
 */
object CdcCanal {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val kafkaHostAndPort = PropertiesUtil.getStrValue("kafka.bootstrap.servers")
    val ddl =
      s"""
        | CREATE TABLE topic_student (
        |   -- schema is totally the same to the MySQL table
        |   id BIGINT,
        |   name STRING,
        |   gender STRING,
        |   age BIGINT,
        |   address STRING
        | ) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'topic_example1',
        |  'properties.bootstrap.servers' = '$kafkaHostAndPort',
        |  'properties.group.id' = 'testGroup',
        |  'format' = 'canal-json'  -- using canal-json as the format
        | )
        |""".stripMargin
    tEnv.executeSql(ddl)

    val query: String = "SELECT id,name,gender,age,address from topic_student"
    val result: Table = tEnv.sqlQuery(query)
    tEnv.toRetractStream[(Long, String, String, Long, String)](result).print()

    env.execute("CDC Canal Example")

  }

}


/**
 * MySQL建表语句
CREATE TABLE `student` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `name` varchar(100) DEFAULT NULL,
  `gender` varchar(4) DEFAULT NULL,
  `age` tinyint(4) DEFAULT NULL,
  `address` varchar(2000) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COMMENT='学生';
 */
