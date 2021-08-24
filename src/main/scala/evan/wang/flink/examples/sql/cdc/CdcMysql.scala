package evan.wang.flink.examples.sql.cdc

import evan.wang.flink.examples.utils.PropertiesUtil
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.streaming.api.scala._

/**
 * FLink CDC 同步Mysql全量及增量数据
 * 流程： (MySQL binlog) ——> (Flink CDC) ——> (Sink)
 */
object CdcMysql {

  def main(args: Array[String]): Unit = {
    val host =  PropertiesUtil.getStrValue("mysql.host")
    val port =  PropertiesUtil.getStrValue("mysql.port")
    val username =  PropertiesUtil.getStrValue("mysql.username")
    val password =  PropertiesUtil.getStrValue("mysql.password")

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(2)
    val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    val ddl =
      s"""
        | CREATE TABLE my_student (
        |   -- schema is totally the same to the MySQL table
        |   id BIGINT NOT NULL,
        |   name STRING,
        |   gender STRING,
        |   age BIGINT,
        |   address STRING,
        |   PRIMARY KEY(id) NOT ENFORCED
        | ) WITH (
        |   'connector' = 'mysql-cdc',
        |   'hostname' = '$host',
        |   'port' = '$port',
        |   'username' = '$username',
        |   'password' = '$password',
        |   'database-name' = 'test',
        |   'table-name' = 'student',
        |   'scan.startup.mode' = 'latest-offset'
        | )
        |""".stripMargin
    // 全量： 'scan.startup.mode' = 'initial'  增量： 'scan.startup.mode' = 'latest-offset'
    tEnv.executeSql(ddl)


    val createSinkSql =
      """
        | CREATE TABLE my_sink (
        |  f0 BIGINT,
        |  f1 STRING,
        |  f2 BIGINT
        | ) WITH (
        |  'connector' = 'print'
        | )
        |""".stripMargin
    tEnv.executeSql(createSinkSql)


    val insertSql = "insert into my_sink select id as f0, name as f1, age as f2 from my_student";
    tEnv.executeSql(insertSql)

//    val query: String = "SELECT id,name,gender,age,address from my_student"
//    val result: Table = tEnv.sqlQuery(query)
//    tEnv.toRetractStream[(Long, String, String, Long, String)](result).print().setParallelism(1)

    env.execute("CDC MySQL Example")
  }

}
