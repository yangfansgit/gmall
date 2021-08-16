package com.yf.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author by yangfan
 * @date 2021/4/10.
 * @desc 操作 Kafka 的工具类
 */
public class MyKafkaUtil {
    /**
     * 消费方法
     * @param topic kafka主题
     * @param groupId kafka组
     * @return
     */
    // kafka服务器
    private static String kafkaServer = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    // 定义kafka默认输出主题
    private static final String DEFAULT_TOPIC="DEFAULT_DATA";
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        // Kafka连接配置
        Properties prop = new Properties();

        prop.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        prop.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        // 将Kafka二进制数据转为Java的对象，需要进行 反序列化操作
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),prop);
    }

    /**
     *
     * @param topic 主题
     * @return 生产者对象
     */
    public static FlinkKafkaProducer<String> getKafkaSink(String topic) {
        return new FlinkKafkaProducer<>(kafkaServer,topic,new SimpleStringSchema());
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> serializationSchema) {
        Properties prop = new Properties();
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        //如果 15 分钟没有更新状态，则超时 默认 1 分钟
        prop.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,1000*60*15+"");
        return new FlinkKafkaProducer<>(DEFAULT_TOPIC, serializationSchema, prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }
}
