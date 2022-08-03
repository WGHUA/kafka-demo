package com.wgh.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author WGH
 * @Date 2022-08-02 22:34
 * @Description kafka消费者
 */
public class ConsumerKafkaTest {

    private static final String KAFKA_TOPIC = "kafka-message";

    public static void main(String[] args) {

        // 添加配置信息
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.200.130:9092");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        /**
         * 设置分组
         * kafka每个组中只有一个消费者可以消费到消息
         * 如果要实现每个组中广播的效果，可以设置每个消费者为一个组，这样每个消费者都可以监听到消息
         */
        //properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group2");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // 订阅主题
        consumer.subscribe(Collections.singletonList(KAFKA_TOPIC));

        while (true) {
            // 拉取消息
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record.key() + ":" + record.value());
            }
        }

    }
}
