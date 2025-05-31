package com.nexuscale.consumer;

import com.nexuscale.config.ConfigManager;
import com.nexuscale.redis.RedisManager;
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
    
    public RedisConsumer(String topic, RedisManager redisManager) {
        this.topic = topic;
        this.redisManager = redisManager;
        this.pollingInterval = ConfigManager.getIntProperty("app.polling.interval", 1000);
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
        
        // Here you can add additional processing logic for the consumed data
        // For example, data validation, transformation, or sending to other systems
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