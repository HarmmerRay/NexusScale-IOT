package com.nexuscale.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nexuscale.database.DatabaseManager;
import com.nexuscale.hbase.HBaseManager;
import com.nexuscale.kafka.KafkaProducerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SensorDataGeneratorService {
    private static final Logger logger = LoggerFactory.getLogger(SensorDataGeneratorService.class);
    
    private final DatabaseManager databaseManager;
    private final HBaseManager hbaseManager;
    private final KafkaProducerManager kafkaProducer;
    private final ObjectMapper objectMapper;
    private final Random random;
    private final ScheduledExecutorService executorService;
    
    public SensorDataGeneratorService(DatabaseManager databaseManager, 
                                    HBaseManager hbaseManager, 
                                    KafkaProducerManager kafkaProducer) {
        this.databaseManager = databaseManager;
        this.hbaseManager = hbaseManager;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        this.executorService = Executors.newScheduledThreadPool(5);
    }
    
    public void startDataGeneration(String deviceId, int state) {
        if (state == 1) {
            logger.info("Device {} is turned ON. Starting data generation...", deviceId);
            startDataGenerationForDevice(deviceId);
        } else {
            logger.info("Device {} is turned OFF. Stopping data generation...", deviceId);
            // 可以在这里实现停止数据生成的逻辑
        }
    }
    
    private void startDataGenerationForDevice(String deviceId) {
        // 在新线程中执行数据生成
        executorService.submit(() -> {
            try {
                // 查询设备信息和模板
                DeviceInfo deviceInfo = getDeviceInfo(deviceId);
                if (deviceInfo == null) {
                    logger.error("Device {} not found in database", deviceId);
                    return;
                }
                
                logger.info("Found device {}: template={}, en_name={}", 
                           deviceId, deviceInfo.template, deviceInfo.enName);
                
                // 开始定期生成数据
                scheduleDataGeneration(deviceInfo);
                
            } catch (Exception e) {
                logger.error("Error starting data generation for device {}", deviceId, e);
            }
        });
    }
    
    private void scheduleDataGeneration(DeviceInfo deviceInfo) {
        // 每10秒生成一次数据
        executorService.scheduleAtFixedRate(() -> {
            try {
                generateAndStoreSensorData(deviceInfo);
            } catch (Exception e) {
                logger.error("Error generating sensor data for device {}", deviceInfo.deviceId, e);
            }
        }, 0, 10, TimeUnit.SECONDS);
    }
    
    private void generateAndStoreSensorData(DeviceInfo deviceInfo) {
        try {
            long timestamp = System.currentTimeMillis();
            
            // 根据设备类型生成完整的JSON数据
            Map<String, Object> sensorDataJson = generateCompleteJsonByType(deviceInfo.enName, deviceInfo.template);
            
            // 创建要存储到HBase的数据 - 只存储设备类型对应的列
            Map<String, String> hbaseData = new HashMap<>();
            
            // 将完整的JSON作为一个列存储，列名就是设备类型
            String jsonValue = objectMapper.writeValueAsString(sensorDataJson);
            hbaseData.put(deviceInfo.enName, jsonValue);
            
            // 创建Kafka消息
            Map<String, Object> kafkaMessage = new HashMap<>();
            kafkaMessage.put("deviceId", deviceInfo.deviceId);
            kafkaMessage.put("deviceType", deviceInfo.enName);
            kafkaMessage.put("timestamp", timestamp);
            kafkaMessage.put("data", sensorDataJson);
            
            String jsonMessage = objectMapper.writeValueAsString(kafkaMessage);
            
            // 发送到Kafka
            kafkaProducer.sendSensorData(deviceInfo.deviceId, jsonMessage);
            
            // 存储到HBase
            hbaseManager.putSensorData(deviceInfo.deviceId, timestamp, hbaseData);
            
            logger.info("Generated and stored sensor data for device {} ({}): {}", 
                       deviceInfo.deviceId, deviceInfo.enName, sensorDataJson);
            
        } catch (Exception e) {
            logger.error("Error generating sensor data for device {}", deviceInfo.deviceId, e);
        }
    }
    
    private Map<String, Object> generateCompleteJsonByType(String enName, String template) {
        Map<String, Object> data = new HashMap<>();
        
        switch (enName.toLowerCase()) {
            case "temperature":
                Map<String, Object> temperatureData = new HashMap<>();
                temperatureData.put("unit", "°C");
                temperatureData.put("value", Double.parseDouble(String.format("%.1f", -10.0 + random.nextDouble() * 50.0)));
                temperatureData.put("range", "-10~40");
                temperatureData.put("accuracy", "±0.1");
                temperatureData.put("battery_level", 80 + random.nextInt(20));
                temperatureData.put("signal_strength", -30 - random.nextInt(40));
                data.put("temperature", temperatureData);
                break;
                
            case "humidity":
                Map<String, Object> humidityData = new HashMap<>();
                humidityData.put("unit", "%RH");
                humidityData.put("value", Double.parseDouble(String.format("%.1f", 30.0 + random.nextDouble() * 40.0)));
                humidityData.put("range", "0~100");
                humidityData.put("accuracy", "±2%");
                humidityData.put("battery_level", 80 + random.nextInt(20));
                humidityData.put("signal_strength", -30 - random.nextInt(40));
                data.put("humidity", humidityData);
                break;
                
            case "air_component":
                Map<String, Object> airData = new HashMap<>();
                airData.put("co2", Double.parseDouble(String.format("%.0f", 400.0 + random.nextDouble() * 1000.0)));
                airData.put("co2_unit", "ppm");
                airData.put("o2", Double.parseDouble(String.format("%.1f", 18.0 + random.nextDouble() * 5.0)));
                airData.put("o2_unit", "%");
                airData.put("pm25", Double.parseDouble(String.format("%.0f", random.nextDouble() * 100.0)));
                airData.put("pm25_unit", "μg/m³");
                airData.put("accuracy", "±5%");
                airData.put("battery_level", 80 + random.nextInt(20));
                airData.put("signal_strength", -30 - random.nextInt(40));
                data.put("air_component", airData);
                break;
                
            case "soil_npk":
                Map<String, Object> npkData = new HashMap<>();
                npkData.put("nitrogen", Double.parseDouble(String.format("%.0f", 50.0 + random.nextDouble() * 200.0)));
                npkData.put("phosphorus", Double.parseDouble(String.format("%.0f", 20.0 + random.nextDouble() * 100.0)));
                npkData.put("potassium", Double.parseDouble(String.format("%.0f", 100.0 + random.nextDouble() * 300.0)));
                npkData.put("unit", "mg/kg");
                npkData.put("accuracy", "±10%");
                npkData.put("battery_level", 80 + random.nextInt(20));
                npkData.put("signal_strength", -30 - random.nextInt(40));
                data.put("soil_NPK", npkData);
                break;
                
            case "soil_ph":
                Map<String, Object> phData = new HashMap<>();
                phData.put("ph", Double.parseDouble(String.format("%.1f", 5.0 + random.nextDouble() * 3.0)));
                phData.put("unit", "pH");
                phData.put("range", "5.0~8.0");
                phData.put("accuracy", "±0.1");
                phData.put("battery_level", 80 + random.nextInt(20));
                phData.put("signal_strength", -30 - random.nextInt(40));
                data.put("soil_PH", phData);
                break;
                
            case "soil_trace_elements":
                Map<String, Object> traceData = new HashMap<>();
                traceData.put("iron", Double.parseDouble(String.format("%.1f", 10.0 + random.nextDouble() * 50.0)));
                traceData.put("zinc", Double.parseDouble(String.format("%.1f", 1.0 + random.nextDouble() * 10.0)));
                traceData.put("copper", Double.parseDouble(String.format("%.1f", 0.5 + random.nextDouble() * 5.0)));
                traceData.put("manganese", Double.parseDouble(String.format("%.1f", 5.0 + random.nextDouble() * 20.0)));
                traceData.put("unit", "mg/kg");
                traceData.put("accuracy", "±5%");
                traceData.put("battery_level", 80 + random.nextInt(20));
                traceData.put("signal_strength", -30 - random.nextInt(40));
                data.put("soil_trace_elements", traceData);
                break;
                
            case "wind_speed":
                Map<String, Object> speedData = new HashMap<>();
                speedData.put("wind_speed", Double.parseDouble(String.format("%.1f", random.nextDouble() * 15.0)));
                speedData.put("unit", "m/s");
                speedData.put("range", "0~15");
                speedData.put("accuracy", "±0.5");
                speedData.put("battery_level", 80 + random.nextInt(20));
                speedData.put("signal_strength", -30 - random.nextInt(40));
                data.put("wind_speed", speedData);
                break;
                
            case "wind_direction":
                Map<String, Object> directionData = new HashMap<>();
                directionData.put("direction", Double.parseDouble(String.format("%.0f", random.nextDouble() * 360.0)));
                directionData.put("unit", "°");
                directionData.put("range", "0~360");
                directionData.put("accuracy", "±5°");
                directionData.put("battery_level", 80 + random.nextInt(20));
                directionData.put("signal_strength", -30 - random.nextInt(40));
                data.put("wind_direction", directionData);
                break;
                
            case "light_intensity":
                Map<String, Object> lightData = new HashMap<>();
                lightData.put("illuminance", Double.parseDouble(String.format("%.0f", random.nextDouble() * 80000.0)));
                lightData.put("unit", "lux");
                lightData.put("range", "0~80000");
                lightData.put("accuracy", "±10%");
                lightData.put("battery_level", 80 + random.nextInt(20));
                lightData.put("signal_strength", -30 - random.nextInt(40));
                data.put("light_intensity", lightData);
                break;
                
            default:
                // 默认生成通用数据
                Map<String, Object> defaultData = new HashMap<>();
                defaultData.put("value", Double.parseDouble(String.format("%.2f", random.nextDouble() * 100.0)));
                defaultData.put("unit", "unknown");
                defaultData.put("status", "active");
                defaultData.put("battery_level", 80 + random.nextInt(20));
                defaultData.put("signal_strength", -30 - random.nextInt(40));
                data.put(enName, defaultData);
                break;
        }
        
        return data;
    }
    
    private DeviceInfo getDeviceInfo(String deviceId) {
        String sql = "SELECT d.device_id, d.device_name, dt.en_name, dt.template " +
                    "FROM device d " +
                    "JOIN device_template dt ON d.dt_id = dt.dt_id " +
                    "WHERE d.device_id = ?";
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setString(1, deviceId);
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                DeviceInfo info = new DeviceInfo();
                info.deviceId = rs.getString("device_id");
                info.deviceName = rs.getString("device_name");
                info.enName = rs.getString("en_name");
                info.template = rs.getString("template");
                return info;
            }
            
        } catch (SQLException e) {
            logger.error("Error querying device info for device {}", deviceId, e);
        }
        
        return null;
    }
    
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("Sensor data generator service shut down");
        }
    }
    
    // 内部类：设备信息
    private static class DeviceInfo {
        String deviceId;
        String deviceName;
        String enName;
        String template;
    }
} 