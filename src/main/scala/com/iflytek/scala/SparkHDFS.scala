package com.iflytek.scala

import org.apache.hadoop.mapred.{KeyValueTextInputFormat, TextInputFormat, TextOutputFormat}
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat
import org.apache.spark.sql.SparkSession

object SparkHDFS {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("SparkHBaseRDD")
      .master("local")
      .getOrCreate()
    import spark.implicits._
    val sc = spark.sparkContext

    val hFile = sc.hadoopFile("test02",
      classOf[KeyValueTextInputFormat],
      classOf[Text],
      classOf[Text])
    hFile.foreach(println)

    var rdd1 = sc.makeRDD(Array(("A",2),("A",1),("B",6),("B",3),("B",7)),2)
    rdd1.saveAsHadoopFile("test01",
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat])
//      classOf[TextOutputFormat[Text,IntWritable]])

    rdd1.saveAsHadoopFile("test02",
      classOf[Text],
      classOf[IntWritable],
      classOf[RDDMultipleTextOutputFormat],
//      classOf[TextOutputFormat[Text,IntWritable]],
      classOf[org.apache.hadoop.io.compress.GzipCodec])
  }

}

class RDDMultipleTextOutputFormat extends MultipleTextOutputFormat[Any, Any] {

  //  DDMultipleTextOutputFormat类中的generateFileNameForKeyValue函数有三个参数，
  //  key和value就是我们RDD的Key和Value，
  //  而name参数是每个Reduce的编号。本例中没有使用该参数，
  //  而是直接将同一个Key的数据输出到同一个文件中。执行：
  override def generateFileNameForKeyValue(key: Any, value: Any, name: String): String =
    key.asInstanceOf[String]

}
