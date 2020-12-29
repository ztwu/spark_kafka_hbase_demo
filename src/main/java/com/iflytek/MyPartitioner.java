package com.iflytek;

import com.google.gson.Gson;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.List;
import java.util.Map;

public class MyPartitioner implements Partitioner {
    Gson gson = new Gson();
    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        List partitions = cluster.partitionsForTopic(s);
        int numPartitions = partitions.size();
        try {
            String value = (String) o1;
            LogEvent logEvent = gson.fromJson(value, LogEvent.class);
            System.out.print("数据==" + value);
            System.out.println(logEvent);
            String eventTime = logEvent.getEventTime();
            int partion =  Math.abs(eventTime.hashCode()%numPartitions);
            System.out.println(String.format("%d,%s", partion,value));
            return partion;
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
