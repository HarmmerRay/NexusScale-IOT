package com.nexuscale.util;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // 配置Kafka生产者的属性
        Properties props = new Properties();
        // 配置Kafka集群的地址，这里需要替换为你实际的虚拟机地址
        props.put("bootstrap.servers", "slave1:9092,slave2:9092,slave3:9092");
        // 配置key的序列化器
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 配置value的序列化器
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建Kafka生产者实例

        try (Producer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                // 模拟MQTT协议格式的数据
                String mqttData = "{\"protocol\": \"MQTT\", \"data\": \"Sample MQTT data\"}";
                // 创建ProducerRecord对象，指定主题和消息
                ProducerRecord<String, String> record = new ProducerRecord<>("iot_topic", mqttData);

                // 发送消息到Kafka集群
                producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.err.println("Failed to send message: " + exception.getMessage());
                        } else {
                            System.out.printf("Sent message to partition %d, offset %d%n",
                                    metadata.partition(), metadata.offset());
                        }
                    }
                });

                // 每隔3秒发送一次消息
                TimeUnit.SECONDS.sleep(3);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        // 关闭生产者
    }
}