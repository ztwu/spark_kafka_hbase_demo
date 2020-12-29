package com.iflytek.scala

import java.util
import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

object sparkrdddemo {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val spark= SparkSession
      .builder
      .master("local")
      .appName("SparkTest")
//      .enableHiveSupport() //# sparkSQL 连接 hive 时需要这句
      .getOrCreate()
    val filePart = "data/data.txt"
    val rdd= spark.sparkContext.textFile(filePart)
    val lines= rdd.flatMap(x => x.split(" ")).collect().toList
    val temp = rdd.flatMap(x=>x.split(" ")).map(x=>{
      println("test==============",x)
      (x,1)}
    )

//    temp.cache()
//    temp.persist(StorageLevel.MEMORY_ONLY)
    spark.sparkContext.setCheckpointDir("data/checkpoint")
    temp.checkpoint()

//    temp.reduceByKey(_+_).foreach(x=>println(x))
//    temp.groupByKey().map(x=>(x._1,x._2.sum)).foreach(x=>println(x))
//    temp.aggregateByKey(0)(
//      (a: Int,b: Int)=>{ a+b },
//      (x: Int,y: Int)=>{ x+y }
//    ).foreach(x=>println(x))
//    val new_temp = temp.combineByKey(
//      (v: Int) => { v + 10 },
//      (c: Int, v: Int) => { c + v },
//      (c1: Int, c2: Int) => { c1 + c2 }
//    )
    temp.foreach(x=>println("1:==",x))
    temp.foreach(x=>println("2:==",x))
    temp.foreachPartition(x=>{
      x.foreach(x=>println("3:==",x))
    })

//    val datas = List("aaa","bbb","ccc")
//    val data222 = spark.sparkContext.parallelize(datas)
//    println(data222)
//    data222.foreachPartition(partition =>
//      {
//        val props = new util.HashMap[String, Object]()
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092")
//        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//          "org.apache.kafka.common.serialization.StringSerializer")
//        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//          "org.apache.kafka.common.serialization.StringSerializer")
//        val producer = new KafkaProducer[String,String](props)
//        partition.foreach{
//          case x:String=>{
//            val message=new ProducerRecord[String, String]("mykafka",null,x)
//            producer.send(message)
//          }
//        }
//      }
//
//    )

//    //广播KafkaSink
//    val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
//      val kafkaProducerConfig = {
//        val p = new Properties()
//        p.setProperty("bootstrap.servers", "kafka:9092")
//        p.setProperty("key.serializer", classOf[StringSerializer].getName)
//        p.setProperty("value.serializer", classOf[StringSerializer].getName)
//        p
//      }
//      spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
//    }
//    data222.foreach(record=>{
//      kafkaProducer.value.send("mykafka", record)
//    })


    println(temp.partitioner)
    println(temp.partitions)


    val testdata = spark.sparkContext.parallelize(List(1,2,2,4))
    val testdata1 = spark.sparkContext.parallelize(List("a"->1,"b"->2,"a"->2))
//    testdata1.foreach(x=>println(x))
//    testdata1.groupByKey().foreach(x=>println(x))
    testdata1.groupBy(_._1).foreach(x=>println(x))

  }
}
