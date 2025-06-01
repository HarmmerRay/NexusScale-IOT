package com.nexuscale.kafka;

import com.nexuscale.config.ConfigManager;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaProducerManager {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerManager.class);
    
    private Producer<String, String> producer;
    private final String topicName;
    
    public KafkaProducerManager() {
        this.topicName = ConfigManager.getProperty("kafka.topic.sensor.data");
        initializeProducer();
    }
    
    private void initializeProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, 
                 ConfigManager.getProperty("kafka.bootstrap.servers"));
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        
        // 生产者配置优化
        props.put(ProducerConfig.RETRIES_CONFIG, 
                 ConfigManager.getIntProperty("kafka.producer.retries", 3));
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 
                 ConfigManager.getIntProperty("kafka.producer.batch.size", 16384));
        props.put(ProducerConfig.LINGER_MS_CONFIG, 
                 ConfigManager.getIntProperty("kafka.producer.linger.ms", 1));
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 
                 ConfigManager.getIntProperty("kafka.producer.buffer.memory", 33554432));
        
        // 确保数据可靠性
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        
        this.producer = new KafkaProducer<>(props);
        logger.info("Kafka producer initialized successfully. Topic: {}", topicName);
    }
    
    public boolean testConnection() {
        try {
            // 发送一个测试消息
            String testMessage = "{\"test\":\"connection\",\"timestamp\":" + System.currentTimeMillis() + "}";
            Future<?> future = producer.send(new ProducerRecord<>(topicName, "test", testMessage));
            future.get(); // 等待发送完成
            
            logger.info("Kafka connection test successful");
            return true;
        } catch (Exception e) {
            logger.error("Kafka connection test failed", e);
            return false;
        }
    }
    
    public void sendSensorData(String deviceId, String jsonData) {
        try {
            String key = "device_" + deviceId;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, jsonData);
            
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("Successfully sent message for device {} to partition {} at offset {}", 
                               deviceId, metadata.partition(), metadata.offset());
                } else {
                    logger.error("Failed to send message for device {}", deviceId, exception);
                }
            });
            
        } catch (Exception e) {
            logger.error("Error sending sensor data for device {}", deviceId, e);
            throw new RuntimeException("Kafka send operation failed", e);
        }
    }
    
    public void sendSensorDataSync(String deviceId, String jsonData) {
        try {
            String key = "device_" + deviceId;
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, key, jsonData);
            
            producer.send(record).get(); // 同步发送
            logger.info("Successfully sent message for device {} synchronously", deviceId);
            
        } catch (Exception e) {
            logger.error("Error sending sensor data synchronously for device {}", deviceId, e);
            throw new RuntimeException("Kafka sync send operation failed", e);
        }
    }
    
    public void flush() {
        producer.flush();
        logger.debug("Kafka producer flushed");
    }
    
    public void close() {
        if (producer != null) {
            producer.close();
            logger.info("Kafka producer closed");
        }
    }
} 