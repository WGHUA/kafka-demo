package com.wgh.kafka;

import com.sun.javafx.collections.MappingChange;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;
import java.util.Properties;

/**
 * @Author WGH
 * @Date 2022-08-02 22:18
 * @Description kafka生产者
 */
public class ProducerKafkaTest {

    private static final String KAFKA_TOPIC = "kafka-message";

    public static void main(String[] args) {

        // 添加kafka的配置信息
        Properties properties = new Properties();
        // 配置broker信息，集群value用逗号隔开
        properties.put("bootstrap.servers", "192.168.200.130:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.RETRIES_CONFIG, 10);

        // 生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 封装消息
        ProducerRecord<String, String> record = new ProducerRecord<>(KAFKA_TOPIC, "00001",
                "hello kafka, i am kafka message body");
        // 发送消息
        try {
            producer.send(record);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 关闭消息通道
        producer.close();

    }
}
