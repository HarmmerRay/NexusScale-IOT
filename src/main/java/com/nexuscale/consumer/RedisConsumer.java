package com.nexuscale.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexuscale.config.ConfigManager;
import com.nexuscale.redis.RedisManager;
import com.nexuscale.service.SensorDataGeneratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class RedisConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(RedisConsumer.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private final String topic;
    private final RedisManager redisManager;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final long pollingInterval;
    private final ObjectMapper objectMapper;
    private SensorDataGeneratorService sensorDataGenerator;
    
    public RedisConsumer(String topic, RedisManager redisManager) {
        this.topic = topic;
        this.redisManager = redisManager;
        this.pollingInterval = ConfigManager.getIntProperty("app.polling.interval", 1000);
        this.objectMapper = new ObjectMapper();
    }
    
    public void setSensorDataGenerator(SensorDataGeneratorService sensorDataGenerator) {
        this.sensorDataGenerator = sensorDataGenerator;
    }
    
    @Override
    public void run() {
        logger.info("Starting Redis consumer for topic: {}", topic);
        
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try (Jedis jedis = redisManager.getJedis()) {
                // Use BLPOP for blocking left pop with timeout
                List<String> result = jedis.blpop(2, topic);
                
                if (result != null && result.size() > 1) {
                    String data = result.get(1); // First element is the key, second is the value
                    processMessage(data);
                } else {
                    // No data available, continue polling
                    logger.debug("No data available for topic: {}", topic);
                }
                
            } catch (Exception e) {
                if (running.get()) {
                    logger.error("Error consuming from topic: {}", topic, e);
                    try {
                        Thread.sleep(pollingInterval);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        
        logger.info("Redis consumer stopped for topic: {}", topic);
    }
    
    private void processMessage(String data) {
        String timestamp = LocalDateTime.now().format(FORMATTER);
        logger.info("[{}] Topic: {} | Data: {}", timestamp, topic, data);
        
        // 处理设备状态消息
        try {
            JsonNode jsonNode = objectMapper.readTree(data);
            
            // 检查是否为数组格式的消息 [className, dataObject]
            JsonNode messageData = null;
            if (jsonNode.isArray() && jsonNode.size() >= 2) {
                // 数组格式，取第二个元素作为实际数据
                messageData = jsonNode.get(1);
            } else {
                // 直接的对象格式
                messageData = jsonNode;
            }
            
            // 检查是否为设备状态消息
            if (messageData != null && messageData.has("deviceId") && messageData.has("state")) {
                String deviceId = messageData.get("deviceId").asText();
                int state = messageData.get("state").asInt();
                
                logger.info("Received device state message: deviceId={}, state={}", deviceId, state);
                
                // 如果设备状态为1（开启）且有传感器数据生成器，则开始生成数据
                if (sensorDataGenerator != null) {
                    sensorDataGenerator.startDataGeneration(deviceId, state);
                } else {
                    logger.warn("SensorDataGenerator not available for processing device state");
                }
            } else {
                // 处理其他类型的消息
                logger.info("Received general message on topic {}: {}", topic, data);
            }
            
        } catch (Exception e) {
            logger.error("Error processing message: {}", data, e);
        }
    }
    
    public void stop() {
        running.set(false);
        logger.info("Stopping consumer for topic: {}", topic);
    }
    
    public boolean isRunning() {
        return running.get();
    }
    
    public String getTopic() {
        return topic;
    }
} 