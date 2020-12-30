package com.iflytek.scala

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.UUID

import com.iflytek.scala.foreachwriter.JDBCSink
import org.apache.spark.sql.{Encoders, functions}
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.functions._

case class MyEntity(id: String, eventtime: Timestamp)

object structuredemo {
  def main(args: Array[String]): Unit = {
    import org.apache.spark.sql.SparkSession
    val spark = SparkSession.builder.master("local[2]").appName("appName").getOrCreate

//    val df = spark.readStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "kafka:9092")
//      .option("subscribe", "mykafka")
//      .load()

//    val df = spark.readStream
//      .format("socket")
//      .option("host", "localhost")
//      .option("port", 9999)
//      .load()

    val schema: StructType = new StructType()
      .add("value","string")

//    val df = spark.readStream
//      .schema(schema)
//      .format("json")
//      .option("path","datastreaming/json")
//      .load()

//    val df = spark.readStream
//      .schema(schema)
//      .format("csv")
//      .option("delimiter", "#")
//      .option("path","datastreaming/csv")
//      .load()

    val df = spark.readStream
//      text：不需要用户指定schema，其返回的列是只有一个"value"
      .format("text")
      .option("delimiter", "#")
      .option("path","datastreaming/text")
      .load()

//    val df = spark.readStream
//      .format("parquet")
//      .option("path","datastreaming/parquet")
//      .load()

    import spark.implicits._
    import org.apache.spark.sql.functions._
    var sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val data1 = df.selectExpr("CAST(value AS STRING)")
      .as(Encoders.STRING).map(row => {
      println("row: ",row)
        val fields = row.split(",")
        val id = fields(0)
        try{
          val eventtime = new Timestamp(sdf.parse(fields(1)).getTime)
          println((id,eventtime))
          MyEntity(id,eventtime)
        }catch {
          case ex:Exception => {
            println(ex)
            MyEntity(id,null)
          }
        }
      })

    val data = data1.withWatermark("eventtime", "10 minutes")
      .groupBy(
        functions.window(data1.col("eventtime"), "1 minutes", "1 minutes"),
        col("id")
      ).agg(count("id") as "count")
      .withColumn("value",
        concat_ws("_",$"id",$"count"))

//    val query = data.writeStream
//      .outputMode("complete")
//      .format("console")
//      .option("truncate", "false")
//      .trigger(Trigger.ProcessingTime(300))
//      .start()
//    query.awaitTermination()

//    val query = data.select($"id",$"count").writeStream
//      .outputMode("update")
//      .format("org.apache.spark.sql.structured.datasource.MySQLSourceProvider")
//      .option("checkpointLocation", "tmp/MySQLSourceProvider11")
//      .option("user","root")
//      .option("password","root")
//      .option("dbtable","zip_test")
//      .option("url","jdbc:mysql://192.168.56.101:3306/test?useSSL=false&characterEncoding=utf-8")
//      .start()
//    query.awaitTermination()

//    val url ="jdbc:mysql://192.168.56.101:3306/test"
//    val username="root"
//    val password="root"
//    val writer = new JDBCSink(url, username, password)
//    val query = data.select($"id",$"count").writeStream
//      .foreach(writer)
//      .outputMode("update")
//      .trigger(Trigger.ProcessingTime("10 seconds"))
//      .start()
//    query.awaitTermination()

//    val query =
//      data.writeStream
//        .format("memory")
//        .outputMode("complete")
//        .trigger(Trigger.ProcessingTime("25 seconds"))
//        .start()
//    query.awaitTermination()

//    val query =
//      data1.writeStream
//        .format("parquet")
//        .option("path", "mnt/data")
//        .option("checkpointLocation", "mnt/check")
//        .partitionBy("id")
//        .outputMode(OutputMode.Append)
//        .trigger(Trigger.ProcessingTime("25 seconds"))
//        .start()
//    query.awaitTermination()

//    val query =
//      data1.writeStream
//        .format("csv")
//        .option("path", "mnt/csv")
//        .option("delimiter","#")
//        .option("checkpointLocation", "mnt/check2")
//        .partitionBy("id")
//        .outputMode(OutputMode.Append)
//        .trigger(Trigger.ProcessingTime("25 seconds"))
//        .start()
//    query.awaitTermination()

    val query =
      data1.select(concat_ws(",",$"id", $"eventtime").cast(StringType).as("test")).writeStream
        .format("text")
        .option("path", "mnt/text2")
        .option("delimiter","#")
        .option("checkpointLocation", "mnt/check6")
//        .partitionBy("value")
        .outputMode(OutputMode.Append)
        .trigger(Trigger.ProcessingTime("25 seconds"))
        .start()
    query.awaitTermination()

//    val query = data1.writeStream
////      .format("parquet")
//      .format("csv")
//      //.format("orc")
//      // .format("json")
//      .option("path", "data/sink")
//      .option("delimiter",",")
//      .option("checkpointLocation", "tmp/temporary-" + UUID.randomUUID.toString)
//      .outputMode(OutputMode.Append)
//      .start()
//    query.awaitTermination()

    // 写入单个Topic
    // 通过option指定Topic
//    val query = data.select($"value")
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "kafka:9092")
//      .option("topic", "topicztwu4")
//      .option("checkpointLocation", "checkpointdata")
//      .outputMode("update")
//      .trigger(Trigger.ProcessingTime(300))
//      .start()
//    query.awaitTermination()

    // 写入多个Topic
    // topic取自数据中的topic列
//    val query = data.select($"value",$"topic")
//      .writeStream
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092")
//      .option("checkpointLocation", "...")
//      .outputMode("update")
//      .trigger(Trigger.ProcessingTime("2 seconds"))
//      .start()
//    query.awaitTermination()


  }

}
