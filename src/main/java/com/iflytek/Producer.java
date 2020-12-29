package com.iflytek;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {
    public static String topic = "duanjt_test";//定义主题

    public static void main(String[] args) throws InterruptedException {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka:9092");//kafka地址，多个地址用逗号分割
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.BATCH_SIZE_CONFIG,20000);
        p.put(ProducerConfig.LINGER_MS_CONFIG, 1000);
        p.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.iflytek.MyPartitioner");
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(p);
        try {
            while (true) {
                String msg = null;
                StringBuffer msgBuff = new StringBuffer();
                msgBuff.append("{");
                msgBuff.append("'userId':'001'");
                msgBuff.append(",");
                msgBuff.append("'userName':'ztwu'");
                msgBuff.append(",");
                msgBuff.append("'eventId':'000001'");
                msgBuff.append(",");
                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                Date now = new Date();
                Calendar calendar = Calendar.getInstance();
                calendar.setTime(now);
                calendar.add(Calendar.DAY_OF_MONTH, -new Random().nextInt(20));
                msgBuff.append(String.format("'eventTime':'%s'", df.format(calendar.getTime())));
                msgBuff.append(",");
                msgBuff.append("'area':'安徽'");
                msgBuff.append(",");
                msgBuff.append("'ip':'192.168.5.6.101'");
                msgBuff.append(",");
                msgBuff.append("'mapAddress':'DF-CF-SS-FF-AE-FC'");
                msgBuff.append("}");
                msg = msgBuff.toString();
//                String msg = "Hello," + new Random().nextInt(100);
                System.out.println("消息:" + msg);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, msg);
                try{
                    Future future = kafkaProducer.send(record);
//                    future.get();//不关心是否发送成功，则不需要这行。
                } catch(Exception e) {
                    e.printStackTrace();//连接错误、No Leader错误都可以通过重试解决；消息太大这类错误kafkaProducer不会进行任何重试，直接抛出异常
                }
                Thread.sleep(500);
            }
        } catch (Exception e){
            System.out.println(e);
        }finally {
            kafkaProducer.close();
        }

    }
}
