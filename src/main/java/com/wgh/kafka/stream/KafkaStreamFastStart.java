package com.wgh.kafka.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

/**
 * 需求：
 * 接收kafka消息内容并计算消息内单词的个数
 * 如：
 * hello kafka stareams
 * hello wgh kafka
 * hello beijing wgh kafka
 * <p>
 * 结果：
 * hello  3
 * kafka 3
 * streams 1
 * wgh 2
 * beijing 1
 * @author WGH
 */
public class KafkaStreamFastStart {

    public static void main(String[] args) {
        //kafka配置信息
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.200.130:9092");
        prop.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-faststart");

        //stream构建器
        StreamsBuilder builder = new StreamsBuilder();

        //流式计算
        group(builder);

        //创建kafkaStream
        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), prop);

        //开启kafka流计算
        kafkaStreams.start();
    }

    /**
     * 实时流式计算
     *
     * @param builder
     */
    private static void group(StreamsBuilder builder) {
        //接收上游处理器的消息
        KStream<String, String> stream = builder.stream("input_topic");
        KStream<String, String> map = stream.flatMapValues(new ValueMapper<String, Iterable<String>>() {
            /**
             * 把消息中的词组，转换为一个一个的单词放到集合中
             * @param value
             * @return
             */
            @Override
            public Iterable<String> apply(String value) {
                return Arrays.asList(value.split(" "));
            }
        }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
            /**
             * 把消息的key,重新赋值，目前消息的key,就是一个个的单词，把单词作为key进行聚合
             * @param key
             * @param value
             * @return
             */
            @Override
            public KeyValue<String, String> apply(String key, String value) {
                return new KeyValue<>(value, value);
            }
        })
                //根据key进行分组  目前的key 就是value,就是一个个的单词
                .groupByKey()
                //聚合的时间窗口  多久聚合一次
                .windowedBy(TimeWindows.of(10000))
                //聚合  求单词的个数，调用count后，消息的vlaue是聚合单词后的统计数值  是一个long类型
                //Materialized.as("count-article-num-001")  是当前消息的状态值，不重复即可
                .count(Materialized.as("count-article-num-001"))
                //转换成 Kstream
                .toStream()
                //把处理后的key和value转成string
                .map((key, value) -> {
                    return new KeyValue<>(key.key().toString(), value.toString());
                });
        //处理后的结果，发送给下游处理器
        map.to("out_topic");
    }
}