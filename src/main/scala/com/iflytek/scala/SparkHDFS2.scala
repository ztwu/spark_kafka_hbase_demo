package com.iflytek.scala

import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.mapreduce.lib.input.{KeyValueTextInputFormat, TextInputFormat}
import org.apache.spark.sql.SparkSession

object SparkHDFS2 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkHBaseRDD")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val hFile = sc.newAPIHadoopFile("test03",
      classOf[KeyValueTextInputFormat],
      classOf[Text],
      classOf[Text])
    hFile.foreach(println)

    var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)),2)
    rdd1.saveAsNewAPIHadoopFile("test03",
      classOf[Text],
      classOf[IntWritable],
      classOf[TextOutputFormat[Text,IntWritable]])

    val hadoopConf = sc.hadoopConfiguration
    hadoopConf.set("mapred.output.compress", "true")
    hadoopConf.set("mapred.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec")
    hadoopConf.set("mapred.output.compression.type", CompressionType.BLOCK.toString)

    var rdd2 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)),2)
    rdd2.saveAsNewAPIHadoopFile("test04",
      classOf[Text],
      classOf[IntWritable],
      classOf[TextOutputFormat[Text,IntWritable]],
      hadoopConf)

  }

}
