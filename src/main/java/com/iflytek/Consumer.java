package com.iflytek;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

public class Consumer {
    public static String topic = "mykafka";//定义主题

    public static void main(String[] args) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        p.put(ConsumerConfig.GROUP_ID_CONFIG, "ztwuuu02");
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
//        p.put(ConsumerConfig.GROUP_ID_CONFIG, "ztwuuu2");
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(p);

//        Collection<PartitionInfo> partitionInfos = kafkaConsumer.partitionsFor("mykafka");
////        指定offset消费
//        List<TopicPartition> list = new ArrayList<TopicPartition>();
//        for(PartitionInfo partitionInfo : partitionInfos){
//            TopicPartition partition = new TopicPartition(topic, partitionInfo.partition());
//            list.add(partition);
//        }
//        kafkaConsumer.assign(list);
//        for(TopicPartition topicPartition:list){
//            kafkaConsumer.seek(topicPartition,0);
//        }
////        kafkaConsumer.seekToBeginning(list);
//        while (true) {
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(String.format("topic:%s,partition:%s,offset:%d,消息:%s", //
//                        record.topic(), record.partition(), record.offset(), record.value()));
//            }
//        }

//        TopicPartition topicPartition = new TopicPartition(topic,0);
//        kafkaConsumer.assign(Arrays.asList(topicPartition));
//        kafkaConsumer.seek(topicPartition,0);
//        while (true) {
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(String.format("topic:%s,offset:%d,消息:%s", //
//                        record.topic(), record.offset(), record.value()));
//            }
//        }

//        订阅自动消费
        kafkaConsumer.subscribe(Arrays.asList("mykafka"));;// 订阅消息
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(String.format("topic:%s,offset:%d,消息:%s", //
                        record.topic(), record.offset(), record.value()));
            }
        }


////        指定时间消费
//        List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>();
//        Map<TopicPartition, Long> timestampsToSearch = new HashMap<TopicPartition, Long>();
//        DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//        Date now = new Date();
//        long nowTime = now.getTime();
//        System.out.println("当前时间: " + df.format(now));
//        long fetchDataTime = nowTime - 1000 * 60 * 30;  // 计算30分钟之前的时间戳
//        for(PartitionInfo partitionInfo : partitionInfos) {
//            topicPartitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
//            timestampsToSearch.put(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()), fetchDataTime);
//        }
//
//        kafkaConsumer.assign(topicPartitions);
//
//        // 获取每个partition一个小时之前的偏移量
//        Map<TopicPartition, OffsetAndTimestamp> map = kafkaConsumer.offsetsForTimes(timestampsToSearch);
//        OffsetAndTimestamp offsetTimestamp = null;
//        System.out.println("开始设置各分区初始偏移量...");
//        for(Map.Entry<TopicPartition, OffsetAndTimestamp> entry : map.entrySet()) {
//            // 如果设置的查询偏移量的时间点大于最大的索引记录时间，那么value就为空
//            offsetTimestamp = entry.getValue();
//            if(offsetTimestamp != null) {
//                int partition = entry.getKey().partition();
//                long timestamp = offsetTimestamp.timestamp();
//                long offset = offsetTimestamp.offset();
//                System.out.println("partition = " + partition +
//                        ", time = " + df.format(new Date(timestamp))+
//                        ", offset = " + offset);
//                // 设置读取消息的偏移量
//                kafkaConsumer.seek(entry.getKey(), offset);
//            }
//        }
//        System.out.println("设置各分区初始偏移量结束...");
//
//        while (true) {
//            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
//            for (ConsumerRecord<String, String> record : records) {
//                System.out.println(String.format("topic:%s,partition:%s,offset:%d,消息:%s", //
//                        record.topic(), record.partition(), record.offset(), record.value()));
//            }
//        }

    }
}