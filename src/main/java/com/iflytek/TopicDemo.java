package com.iflytek;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import org.apache.kafka.common.security.JaasUtils;
import scala.collection.JavaConversions;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class TopicDemo {

    public static void topicAddPartition(){
        ZkUtils zkUtils = ZkUtils.apply("192.168.56.101:2181",
                30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        AdminUtils.addPartitions(zkUtils, "t1", 2, null,
                false, RackAwareMode.Enforced$.MODULE$);
    }

    public static void topicList(){
        ZkUtils zkUtils = ZkUtils.apply("192.168.56.101:2181",
                30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        List<String> topics = JavaConversions.seqAsJavaList(zkUtils.getAllTopics());
        for(String topic:topics){
            System.out.println(topic);
        }
    }

    public static void create(){
        ZkUtils zkUtils = ZkUtils.apply("192.168.56.101:2181",
                30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        // 创建一个单分区单副本名为t1的topic
        AdminUtils.createTopic(zkUtils, "t1", 1, 1,
                new Properties(), RackAwareMode.Enforced$.MODULE$);
        zkUtils.close();
    }

    public static void delete(){
        ZkUtils zkUtils = ZkUtils.apply("192.168.56.101:2181",
                30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        // 删除topic 't1'
        AdminUtils.deleteTopic(zkUtils, "t1");
        zkUtils.close();
    }

    public static void query(){
        ZkUtils zkUtils = ZkUtils.apply("192.168.56.101:2181",
                30000, 30000,
                JaasUtils.isZkSecurityEnabled());
        // 获取topic 'test'的topic属性属性
        Properties props = AdminUtils.fetchEntityConfig(zkUtils,
                ConfigType.Topic(), "duanjt_test");
        // 查询topic-level属性
        Iterator it = props.entrySet().iterator();
        while(it.hasNext()){
            Map.Entry entry=(Map.Entry)it.next();
            Object key = entry.getKey();
            Object value = entry.getValue();
            System.out.println(key + " = " + value);
        }
        zkUtils.close();
    }

    public static void update(){
        ZkUtils zkUtils = ZkUtils.apply("192.168.56.101:2181", 30000, 30000, JaasUtils.isZkSecurityEnabled());
        Properties props = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), "test");
        // 增加topic级别属性
        props.put("min.cleanable.dirty.ratio", "0.3");
        // 删除topic级别属性
        props.remove("max.message.bytes");
        // 修改topic 'test'的属性
        AdminUtils.changeTopicConfig(zkUtils, "test", props);
        zkUtils.close();

    }

    public static void main(String[] args) {
//        create();
        query();
//        delete();
        topicList();

    }

}
