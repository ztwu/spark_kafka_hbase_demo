package com.iflytek.scala

import org.apache.spark.sql.{SaveMode, SparkSession}

object sinkdemo {
  def main(args: Array[String]): Unit = {

    val spark= SparkSession
      .builder
      .master("local")
      .appName("SparkTest")
      .getOrCreate()

    import  spark.implicits._

    val data = spark.createDataFrame(List(
      ("json",23,"running"),
      ("charles",32,"basketball"),
      ("tom",28,"football"),
      ("lili",24,"running"),
      ("bob",23,"swimming")
    )).toDF("name","age","hobby")
    println(data)

//    data.write
//      .format("json")
//      .mode(SaveMode.Overwrite)
//      .save("output.json")

//    data.write
//      .format("csv")
//      .mode(SaveMode.Overwrite)
//      .save("output.csv")

//    delimiter 分隔符，默认为逗号,
//    nullValue 指定一个字符串代表 null 值
//      quote 引号字符，默认为双引号"
//      header 第一行不作为数据内容，作为标题
//      inferSchema 自动推测字段类型
//    data.write
//      .format("csv")
//      .mode(SaveMode.Overwrite)
//      .option("delimiter", "/")
//      .option("inferSchema", true)
//      .option("header", true)
//      .save("output2.csv")

    /**
      * +---------------------+
      * |value                |
      * +---------------------+
      * |name,age,hobby       |
      * |json,23,running      |
      * |charles,32,basketball|
      * |tom,28,football      |
      * |lili,24,running      |
      * |bob,20,swimming      |
      * +---------------------+
      *
      */
    //
//    import org.apache.spark.sql.functions._
////    val allClumnName: String = data.columns.mkString(",")
////    val data_new = data.selectExpr(s"concat_ws(',',$allClumnName)")
//    val data_new = data.select(concat_ws(",",$"name",$"age",$"hobby"))
//    data_new.write
//      .format("text")
//      .mode(SaveMode.Overwrite)
//      .save("output.text")

//    data.write
//      .format("parquet")
//      .mode(SaveMode.Overwrite)
////      .partitionBy("name")
//      .save("output.parquet")

//    data.write
//      .format("jdbc")
//      .mode(SaveMode.Overwrite)
//      .option("url", "jdbc:mysql://localhost:3306/tp_manage")
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("dbtable", "user")
//      .option("user", "localhost")
//      .option("password", "localhost")
//      .save()

//    data.write.
//      format("org.apache.spark.sql.execution.datasources.hbase")
//      .option("hbase.table.rowkey.field", "name")
//      .option("hbase.table.name", "hbase_table")
//      .option("hbase.zookeeper.quorum", "XXX:2181")
//      .option("hbase.table.rowkey.prefix", "00")
//      .option("hbase.table.numReg", "12")
//      .option("bulkload.enable", "false")
//      .save()

  }

}
