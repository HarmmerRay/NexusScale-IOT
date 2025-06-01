package com.nexuscale.service;

import com.nexuscale.config.ConfigManager;
import com.nexuscale.consumer.RedisConsumer;
import com.nexuscale.database.DatabaseManager;
import com.nexuscale.hbase.HBaseManager;
import com.nexuscale.kafka.KafkaProducerManager;
import com.nexuscale.redis.RedisManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class DeviceDataConsumerService {
    private static final Logger logger = LoggerFactory.getLogger(DeviceDataConsumerService.class);
    
    private final DatabaseManager databaseManager;
    private final RedisManager redisManager;
    private final HBaseManager hbaseManager;
    private final KafkaProducerManager kafkaProducer;
    private final SensorDataGeneratorService sensorDataGenerator;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, RedisConsumer> consumers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Future<?>> consumerTasks = new ConcurrentHashMap<>();
    
    public DeviceDataConsumerService() {
        this.databaseManager = new DatabaseManager();
        this.redisManager = new RedisManager();
        this.hbaseManager = new HBaseManager();
        this.kafkaProducer = new KafkaProducerManager();
        this.sensorDataGenerator = new SensorDataGeneratorService(databaseManager, hbaseManager, kafkaProducer);
        
        int threadCount = ConfigManager.getIntProperty("app.consumer.threads", 10);
        this.executorService = Executors.newFixedThreadPool(threadCount);
        
        logger.info("Device Data Consumer Service initialized with {} threads", threadCount);
    }
    
    public void start() {
        logger.info("Starting Device Data Consumer Service...");
        
        // Test connections
        if (!testConnections()) {
            logger.error("Connection tests failed. Service cannot start.");
            return;
        }
        
        // 初始化所有状态为1的活跃设备
        logger.info("Initializing active devices from database...");
        sensorDataGenerator.initializeActiveDevices();
        
        // Get device types from database
        List<String> deviceTypes = databaseManager.getDeviceTypes();
        
        if (deviceTypes.isEmpty()) {
            logger.warn("No device types found in database. No consumers will be started.");
            return;
        }
        
        // Start consumer for each device type
        for (String deviceType : deviceTypes) {
            startConsumerForTopic(deviceType);
        }
        
        logger.info("All consumers started successfully. Total consumers: {}", consumers.size());
        logger.info("Active devices initialized: {}", sensorDataGenerator.getActiveDeviceCount());
    }
    
    private boolean testConnections() {
        logger.info("Testing database connection...");
        if (!databaseManager.testConnection()) {
            logger.error("Database connection failed");
            return false;
        }
        
        logger.info("Testing Redis connection...");
        if (!redisManager.testConnection()) {
            logger.error("Redis connection failed");
            return false;
        }
        
        logger.info("Testing HBase connection...");
        if (!hbaseManager.testConnection()) {
            logger.error("HBase connection failed");
            return false;
        }
        
        logger.info("Testing Kafka connection...");
        if (!kafkaProducer.testConnection()) {
            logger.error("Kafka connection failed");
            return false;
        }
        
        logger.info("All connection tests passed");
        return true;
    }
    
    private void startConsumerForTopic(String topic) {
        if (consumers.containsKey(topic)) {
            logger.warn("Consumer for topic {} already exists", topic);
            return;
        }
        
        RedisConsumer consumer = new RedisConsumer(topic, redisManager);
        consumer.setSensorDataGenerator(sensorDataGenerator);
        consumers.put(topic, consumer);
        
        Future<?> task = executorService.submit(consumer);
        consumerTasks.put(topic, task);
        
        logger.info("Started consumer for topic: {}", topic);
    }
    
    public void stopConsumerForTopic(String topic) {
        RedisConsumer consumer = consumers.remove(topic);
        if (consumer != null) {
            consumer.stop();
            
            Future<?> task = consumerTasks.remove(topic);
            if (task != null) {
                task.cancel(true);
            }
            
            logger.info("Stopped consumer for topic: {}", topic);
        }
    }
    
    public void refreshDeviceTypes() {
        logger.info("Refreshing device types from database...");
        
        List<String> currentDeviceTypes = databaseManager.getDeviceTypes();
        
        // Stop consumers for topics that no longer exist
        for (String existingTopic : consumers.keySet()) {
            if (!currentDeviceTypes.contains(existingTopic)) {
                stopConsumerForTopic(existingTopic);
                logger.info("Removed consumer for obsolete topic: {}", existingTopic);
            }
        }
        
        // Start consumers for new topics
        for (String deviceType : currentDeviceTypes) {
            if (!consumers.containsKey(deviceType)) {
                startConsumerForTopic(deviceType);
                logger.info("Added consumer for new topic: {}", deviceType);
            }
        }
        
        logger.info("Device types refresh completed. Active consumers: {}", consumers.size());
    }
    
    public void shutdown() {
        logger.info("Shutting down Device Data Consumer Service...");
        
        // Stop all consumers
        for (RedisConsumer consumer : consumers.values()) {
            consumer.stop();
        }
        
        // Cancel all tasks
        for (Future<?> task : consumerTasks.values()) {
            task.cancel(true);
        }
        
        // Shutdown services
        sensorDataGenerator.shutdown();
        
        // Shutdown executor service
        executorService.shutdown();
        
        // Close connections
        redisManager.close();
        hbaseManager.close();
        kafkaProducer.close();
        
        logger.info("Device Data Consumer Service shut down completed");
    }
    
    public int getActiveConsumerCount() {
        return (int) consumers.values().stream().filter(RedisConsumer::isRunning).count();
    }
    
    public void printStatus() {
        logger.info("=== Device Data Consumer Service Status ===");
        logger.info("Total consumers: {}", consumers.size());
        logger.info("Active consumers: {}", getActiveConsumerCount());
        
        for (String topic : consumers.keySet()) {
            RedisConsumer consumer = consumers.get(topic);
            logger.info("Topic: {} - Status: {}", topic, consumer.isRunning() ? "RUNNING" : "STOPPED");
        }
        logger.info("==========================================");
    }
} 