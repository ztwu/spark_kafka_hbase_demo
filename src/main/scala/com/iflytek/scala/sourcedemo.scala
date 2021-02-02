package com.iflytek.scala

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

object sourcedemo {

  def main(args: Array[String]): Unit = {
    val spark= SparkSession
      .builder
      .master("local")
      .appName("SparkTest")
      .getOrCreate()

    import  spark.implicits._

    val schema: StructType = new StructType()
      .add("name","string")
      .add("age","integer")
      .add("hobby","string")

//    val data1 = spark.read
//      .schema(schema)
//      .format("json")
//      .load("data.json")
//    data1.select($"name").foreach(x=>println(x))
//
//    val data2 = spark.read
//      .schema(schema)
//      .format("csv")
//      .option("delimiter", ",")
//      .load("data.csv")
//    data2.select($"name").foreach(x=>println(x))

//    val data3 = spark.read
//      .format("csv")
//      .option("delimiter", ",")
//      .option("inferSchema", true)
//      .option("header", true)
//      .load("data2.csv")
//    data3.select($"name").foreach(x=>println(x))

//    val data4 = spark.read
//      .format("parquet")
//      .load("datatest.parquet")
//    data4.select($"name").foreach(x=>println(x))

    val data5 = spark.read
      .format("text")
      .load("data.txt")
    data5.show(false)
    data5.foreach(x=>println(x))

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

//    val data6 = spark.read
//      .format("jdbc")
//      .option("url", "jdbc:mysql://localhost:3306/tp_manage")
//      .option("driver","com.mysql.jdbc.Driver")
//      .option("dbtable", "user")
//      .option("user", "localhost")
//      .option("password", "localhost")
//      .load()
//    data6.select($"name").show()

  }

}
