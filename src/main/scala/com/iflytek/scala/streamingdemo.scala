package com.iflytek.scala

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object streamingdemo {

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("test01")
      .set("spark.dynamicAllocation.enabled", "false")
      .set("spark.streaming.backpressure.enabled", "false")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.kafka.maxRetries", "3")

    val streamingContext: StreamingContext = new StreamingContext(conf, Seconds(1))

    val kafkaParams: Map[String, Object] = Map[String, Object](
      "bootstrap.servers" -> "kafka:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "test00111",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    /** 可以配置多个 */
    val topics = Array("mykafka")

//    val topicPartition = Array(new TopicPartition("mykafka", 0),
//      new TopicPartition("mykafka", 1),
//      new TopicPartition("mykafka", 2))
//
//    val offsets: Map[TopicPartition, Long] = Map(new TopicPartition("mykafka", 0) -> 0,
//      new TopicPartition("mykafka", 1) -> 0,
//      new TopicPartition("mykafka", 2) -> 0)

//    /** 1、这种订阅会读取所有的partition数据 但是可以指定某些partition的offset */
//    val stream1: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      Subscribe[String, String](topics, kafkaParams, offsets)
//    )
//
    /** 2、这种订阅会读取所有的partition数据 */
    val stream2: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

//    /**
//      * 3、这种订阅指定策略会读取指定的的partition数据
//      *    和指定的offset开始位置
//      */
//    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
//      streamingContext,
//      PreferConsistent,
//      ConsumerStrategies.Assign[String, String](topicPartition, kafkaParams, offsets)
//    )

    stream2.foreachRDD(lineRDD => {
      if (!lineRDD.isEmpty()) {
        val offsetRanges: Array[OffsetRange] = lineRDD.asInstanceOf[HasOffsetRanges].offsetRanges
        lineRDD.foreachPartition(iter => {
          iter.foreach(record => {
            println("partition = " + record.partition() ," key = " + record.key(), " value = " + record.value(), " offset = " + record.offset())
          })
        })
        stream2.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }


}
