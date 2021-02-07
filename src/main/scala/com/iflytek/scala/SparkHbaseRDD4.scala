package com.iflytek.scala

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SparkSession

/**
  * saveAsNewAPIHadoopFile
  *
  */
object SparkHbaseRDD4 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkHBaseRDD")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val indataRDD = sc.makeRDD(Array((1,"jack",15),(2,"Lily",16),(3,"mike",16)))
      .toDF("col_01","col_02","col_03")

    val tableName = "test"

    //1-构建需要的rdd
    val hbaseInfoRDD = indataRDD.rdd.map(x => {
      val col_01= x.getAs[String]("col_01")
      val col_02= x.getAs[String]("col_02")

      val columns = scala.collection.mutable.HashMap[String,String]()
      columns.put("col_01",col_01)
      columns.put("col_02",col_02)
      // HBase API  Put

      val rowkey = "mykey"  // HBase的rowkey
      val put = new Put(Bytes.toBytes(rowkey)) // 要保存到HBase的Put对象

      // 每一个rowkey对应的cf中的所有column字段
      for((k,v) <- columns) {
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(k.toString), Bytes.toBytes(v.toString));
      }

      (new ImmutableBytesWritable(rowkey.getBytes), put)
    })

    //2-调用saveAsNewAPIHadoopFile,进行单表的写入
    val conf = new Configuration()
    conf.set("hbase.rootdir","hdfs://hadoop001:8020/hbase")
    conf.set("hbase.zookeeper.quorum","hadoop001:2181")
    // 设置写数据到哪个表中, 支持多表
    conf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    hbaseInfoRDD.saveAsNewAPIHadoopFile(
      "null",
      classOf[ImmutableBytesWritable],
      classOf[Put],
      classOf[TableOutputFormat[ImmutableBytesWritable]],
      conf
    )

  }
}
