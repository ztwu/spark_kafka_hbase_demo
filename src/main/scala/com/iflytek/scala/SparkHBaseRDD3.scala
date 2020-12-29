package com.iflytek.scala

import org.apache.commons.codec.digest.DigestUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat2, LoadIncrementalHFiles, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, KeyValue, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ListBuffer

object SparkHBaseRDD3 {

  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("insertWithBulkLoad").master("local[4]").getOrCreate()
    val sc = sparkSession.sparkContext

    val tableName = "test"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(HConstants.ZOOKEEPER_QUORUM, "192.168.56.101")
    hbaseConf.set(HConstants.ZOOKEEPER_CLIENT_PORT, "2181")
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)

    val conn = ConnectionFactory.createConnection(hbaseConf)
    val admin = conn.getAdmin
    val table = conn.getTable(TableName.valueOf(tableName))

    val job = Job.getInstance(hbaseConf)
    //设置job的输出格式
    job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setMapOutputValueClass(classOf[KeyValue])
    job.setOutputFormatClass(classOf[HFileOutputFormat2])
    HFileOutputFormat2.configureIncrementalLoad(job, table, conn.getRegionLocator(TableName.valueOf(tableName)))

    val indataRDD = sc.makeRDD(Array("11,jack,15","21,Lily,16","31,mike,16"))
    val rdd = indataRDD.map(_.split(',')).flatMap(arr=>{
      val listBuffer = new ListBuffer[(ImmutableBytesWritable, KeyValue)]
      val kv1: KeyValue = new KeyValue(Bytes.toBytes(arr(0)), Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes(arr(1)))
      val kv2: KeyValue = new KeyValue(Bytes.toBytes(arr(0)), Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes(arr(2)))
      listBuffer.append((new ImmutableBytesWritable, kv2))
      listBuffer.append((new ImmutableBytesWritable, kv1))
      listBuffer
    })
    rdd.saveAsNewAPIHadoopFile("hdfs://192.168.56.101:9000/test", classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat2], job.getConfiguration)
    val bulkLoader = new LoadIncrementalHFiles(hbaseConf)
    bulkLoader.doBulkLoad(new Path("hdfs://192.168.56.101:9000/test"), admin, table, conn.getRegionLocator(TableName.valueOf(tableName)))
  }

}
