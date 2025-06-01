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
        
        try {
            // 解析template JSON
            JsonNode templateNode = objectMapper.readTree(template);
            
            // 根据设备类型获取对应的模板节点
            JsonNode deviceTypeNode = templateNode.get(enName);
            if (deviceTypeNode == null) {
                logger.warn("No template found for device type: {}", enName);
                return data;
            }
            
            Map<String, Object> generatedData = new HashMap<>();
            
            // 处理不同类型的传感器数据
            switch (enName.toLowerCase()) {
                case "temperature":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                case "humidity":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                case "air_component":
                    // 空气成分通常包含多个元素，需要为每个元素生成值
                    if (deviceTypeNode.has("co2_range")) {
                        generatedData.put("co2_value", generateValueFromRange(deviceTypeNode.get("co2_range")));
                    }
                    if (deviceTypeNode.has("o2_range")) {
                        generatedData.put("o2_value", generateValueFromRange(deviceTypeNode.get("o2_range")));
                    }
                    if (deviceTypeNode.has("pm25_range")) {
                        generatedData.put("pm25_value", generateValueFromRange(deviceTypeNode.get("pm25_range")));
                    }
                    break;
                    
                case "soil_npk":
                    // 土壤NPK包含氮磷钾三个元素
                    if (deviceTypeNode.has("nitrogen_range")) {
                        generatedData.put("nitrogen_value", generateValueFromRange(deviceTypeNode.get("nitrogen_range")));
                    }
                    if (deviceTypeNode.has("phosphorus_range")) {
                        generatedData.put("phosphorus_value", generateValueFromRange(deviceTypeNode.get("phosphorus_range")));
                    }
                    if (deviceTypeNode.has("potassium_range")) {
                        generatedData.put("potassium_value", generateValueFromRange(deviceTypeNode.get("potassium_range")));
                    }
                    break;
                    
                case "soil_ph":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                case "soil_trace_elements":
                    // 土壤微量元素包含多种元素
                    if (deviceTypeNode.has("iron_range")) {
                        generatedData.put("iron_value", generateValueFromRange(deviceTypeNode.get("iron_range")));
                    }
                    if (deviceTypeNode.has("zinc_range")) {
                        generatedData.put("zinc_value", generateValueFromRange(deviceTypeNode.get("zinc_range")));
                    }
                    if (deviceTypeNode.has("copper_range")) {
                        generatedData.put("copper_value", generateValueFromRange(deviceTypeNode.get("copper_range")));
                    }
                    if (deviceTypeNode.has("manganese_range")) {
                        generatedData.put("manganese_value", generateValueFromRange(deviceTypeNode.get("manganese_range")));
                    }
                    break;
                    
                case "wind_speed":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                case "wind_direction":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                case "light_intensity":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                default:
                    // 默认情况，尝试从range字段生成值
                    if (deviceTypeNode.has("range")) {
                        generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    } else {
                        generatedData.put("value", random.nextDouble() * 100.0);
                    }
                    break;
            }
            
            data.put(enName, generatedData);
            
        } catch (Exception e) {
            logger.error("Error parsing template for device type {}: {}", enName, e.getMessage());
            // 如果解析失败，返回默认数据
            Map<String, Object> defaultData = new HashMap<>();
            defaultData.put("value", random.nextDouble() * 100.0);
            data.put(enName, defaultData);
        }
        
        return data;
    }
    
    /**
     * 从range字符串中生成随机值
     * 支持格式：
     * - "0~100" -> 在0到100之间生成随机值
     * - "37" -> 固定值37
     * - "-10~40" -> 在-10到40之间生成随机值
     */
    private double generateValueFromRange(JsonNode rangeNode) {
        if (rangeNode == null || rangeNode.isNull()) {
            return random.nextDouble() * 100.0;
        }
        
        String range = rangeNode.asText();
        
        try {
            if (range.contains("~")) {
                // 处理范围格式：min~max
                String[] parts = range.split("~");
                if (parts.length == 2) {
                    double min = Double.parseDouble(parts[0].trim());
                    double max = Double.parseDouble(parts[1].trim());
                    
                    // 生成范围内的随机值
                    double value = min + random.nextDouble() * (max - min);
                    
                    // 根据数值大小决定保留的小数位数
                    if (max - min > 100) {
                        return Math.round(value * 10.0) / 10.0; // 保留1位小数
                    } else if (max - min > 10) {
                        return Math.round(value * 100.0) / 100.0; // 保留2位小数
                    } else {
                        return Math.round(value * 1000.0) / 1000.0; // 保留3位小数
                    }
                }
            } else {
                // 处理固定值格式
                return Double.parseDouble(range.trim());
            }
        } catch (NumberFormatException e) {
            logger.warn("Invalid range format: {}, using random value", range);
        }
        
        // 如果解析失败，返回随机值
        return random.nextDouble() * 100.0;
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