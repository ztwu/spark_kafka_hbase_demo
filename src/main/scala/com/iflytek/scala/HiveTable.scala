package com.iflytek.scala

import java.io.File
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object HiveTable {
  def main(args: Array[String]): Unit = {

    System.setProperty("HADOOP_USER_NAME","hadoop")

    // warehouseLocation points to the default location for managed databases and tables
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Spark Hive Example")
//      用于指定warehouse的默认数据路径
      .config("spark.sql.warehouse.dir", warehouseLocation)
//      指定临时文件目录
      .config("hive.exec.scratchdir", "D:/")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    import spark.sql

    sql("drop table if EXISTS src")
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) row format delimited fields terminated by ',' stored as textfile")
    sql("LOAD DATA LOCAL INPATH 'data2.txt' INTO TABLE src")

    // Queries are expressed in HiveQL
    sql("SELECT * FROM src").show()
//     +---+-------+
//     |key|  value|
//     +---+-------+
//     |238|val_238|
//     | 86| val_86|
//     |311|val_311|
//     ...

    // Aggregation queries are also supported.
    sql("SELECT COUNT(*) FROM src").show()
    // +--------+
    // |count(1)|
    // +--------+
    // |    500 |
    // +--------+

    // The results of SQL queries are themselves DataFrames and support all normal functions.
    val sqlDF = sql("SELECT key, value FROM src WHERE key < 10 ORDER BY key")

    // The items in DataFrames are of type Row, which allows you to access each column by ordinal.
    val stringsDS = sqlDF.map {
      case Row(key: Int, value: String) => s"Key: $key, Value: $value"
    }
    stringsDS.show()
    // +--------------------+
    // |               value|
    // +--------------------+
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // |Key: 0, Value: val_0|
    // ...

    // You can also use DataFrames to create temporary views within a SparkSession.
    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")

    // Queries can then join DataFrame data with data stored in Hive.
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()
    // +---+------+---+------+
    // |key| value|key| value|
    // +---+------+---+------+
    // |  2| val_2|  2| val_2|
    // |  4| val_4|  4| val_4|
    // |  5| val_5|  5| val_5|
    // ...

    // Create a Hive managed Parquet table, with HQL syntax instead of the Spark SQL native syntax
    // `USING hive`
    sql("drop table if exists hive_records")
    sql("CREATE TABLE if not exists hive_records(key int, value string) STORED AS PARQUET")
    // Save DataFrame to the Hive managed table
    val df = spark.table("src")
    df.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("hive_records")
    // After insertion, the Hive managed table has data now
    sql("SELECT * FROM hive_records").show()
    // +---+-------+
    // |key|  value|
    // +---+-------+
    // |238|val_238|
    // | 86| val_86|
    // |311|val_311|
    // ...

    // Prepare a Parquet data directory
    val dataDir = "/tmp/parquet_data"
    spark.range(10).write.mode(SaveMode.Overwrite).parquet(dataDir)
    // Create a Hive external Parquet table
    sql("drop table if exists hive_ints")
    sql(s"CREATE EXTERNAL TABLE if not exists hive_ints(key int) STORED AS PARQUET LOCATION '$dataDir'")
    // The Hive external table should already have data
    sql("SELECT * FROM hive_ints").show()
    // +---+
    // |key|
    // +---+
    // |  0|
    // |  1|
    // |  2|
    // ...

    // Turn on flag for Hive Dynamic Partitioning
    spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
    spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    // Create a Hive partitioned table using DataFrame API
    df.write
      .partitionBy("key")
      .format("hive")
      .mode(SaveMode.Overwrite)
      .saveAsTable("hive_part_tbl")
    // Partitioned column `key` will be moved to the end of the schema.
    sql("SELECT * FROM hive_part_tbl").show()
    // +-------+---+
    // |  value|key|
    // +-------+---+
    // |val_238|238|
    // | val_86| 86|
    // |val_311|311|
    // ...

    sql("show databases").show()

    spark.stop()
  }
}

case class Record(key: Int, value: String)

