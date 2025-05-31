package com.nexuscale.redis;

import com.nexuscale.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;

public class RedisManager {
    private static final Logger logger = LoggerFactory.getLogger(RedisManager.class);
    
    private final JedisPool jedisPool;
    
    public RedisManager() {
        String host = ConfigManager.getProperty("redis.host", "localhost");
        int port = ConfigManager.getIntProperty("redis.port", 6379);
        int timeout = ConfigManager.getIntProperty("redis.timeout", 2000);
        int database = ConfigManager.getIntProperty("redis.database", 0);
        
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(5);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis(Duration.ofSeconds(60).toMillis());
        poolConfig.setTimeBetweenEvictionRunsMillis(Duration.ofSeconds(30).toMillis());
        
        this.jedisPool = new JedisPool(poolConfig, host, port, timeout, null, database);
        
        logger.info("Redis connection pool initialized - {}:{}, database: {}", host, port, database);
    }
    
    public Jedis getJedis() {
        return jedisPool.getResource();
    }
    
    public boolean testConnection() {
        try (Jedis jedis = getJedis()) {
            String response = jedis.ping();
            logger.info("Redis ping response: {}", response);
            return "PONG".equals(response);
        } catch (Exception e) {
            logger.error("Redis connection test failed", e);
            return false;
        }
    }
    
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            logger.info("Redis connection pool closed");
        }
    }
} 