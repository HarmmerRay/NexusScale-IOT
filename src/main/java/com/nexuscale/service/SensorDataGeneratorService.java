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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class SensorDataGeneratorService {
    private static final Logger logger = LoggerFactory.getLogger(SensorDataGeneratorService.class);
    
    private final DatabaseManager databaseManager;
    private final HBaseManager hbaseManager;
    private final KafkaProducerManager kafkaProducer;
    private final ObjectMapper objectMapper;
    private final Random random;
    private final ScheduledExecutorService executorService;
    
    // 跟踪每个设备的数据生成任务
    private final Map<String, ScheduledFuture<?>> deviceTasks;
    
    public SensorDataGeneratorService(DatabaseManager databaseManager, 
                                    HBaseManager hbaseManager, 
                                    KafkaProducerManager kafkaProducer) {
        this.databaseManager = databaseManager;
        this.hbaseManager = hbaseManager;
        this.kafkaProducer = kafkaProducer;
        this.objectMapper = new ObjectMapper();
        this.random = new Random();
        this.executorService = Executors.newScheduledThreadPool(10);
        this.deviceTasks = new ConcurrentHashMap<>();
    }
    
    public void startDataGeneration(String deviceId, int state) {
        if (state == 1) {
            logger.info("Device {} is turned ON. Starting data generation...", deviceId);
            startDataGenerationForDevice(deviceId);
        } else {
            logger.info("Device {} is turned OFF. Stopping data generation...", deviceId);
            stopDataGenerationForDevice(deviceId);
        }
    }
    
    private void startDataGenerationForDevice(String deviceId) {
        // 如果设备已经在运行，先停止
        stopDataGenerationForDevice(deviceId);
        
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
    
    private void stopDataGenerationForDevice(String deviceId) {
        ScheduledFuture<?> task = deviceTasks.remove(deviceId);
        if (task != null) {
            boolean cancelled = task.cancel(false);
            if (cancelled) {
                logger.info("Successfully stopped data generation for device {}", deviceId);
            } else {
                logger.warn("Failed to cancel data generation task for device {}", deviceId);
            }
        } else {
            logger.info("No active data generation task found for device {}", deviceId);
        }
    }
    
    private void scheduleDataGeneration(DeviceInfo deviceInfo) {
        // 每1分钟生成一次数据
        ScheduledFuture<?> future = executorService.scheduleAtFixedRate(() -> {
            try {
                generateAndStoreSensorData(deviceInfo);
            } catch (Exception e) {
                logger.error("Error generating sensor data for device {}", deviceInfo.deviceId, e);
            }
        }, 0, 1, TimeUnit.MINUTES);
        
        // 将任务保存到映射中，以便后续可以取消
        deviceTasks.put(deviceInfo.deviceId, future);
        logger.info("Scheduled data generation task for device {}", deviceInfo.deviceId);
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
                    if (templateNode.has("co2")) {
                        JsonNode co2Node = templateNode.get("co2");
                        generatedData.put("co2_value", generateValueFromRange(co2Node.get("range")));
                    }
                    if (templateNode.has("o2")) {
                        JsonNode o2Node = templateNode.get("o2");
                        generatedData.put("o2_value", generateValueFromRange(o2Node.get("range")));
                    }
                    if (templateNode.has("pm25")) {
                        JsonNode pm25Node = templateNode.get("pm25");
                        generatedData.put("pm25_value", generateValueFromRange(pm25Node.get("range")));
                    }
                    break;
                    
                case "soil_npk":
                    // 土壤NPK包含氮磷钾三个元素
                    if (templateNode.has("nitrogen")) {
                        JsonNode nitrogenNode = templateNode.get("nitrogen");
                        generatedData.put("nitrogen_value", generateValueFromRange(nitrogenNode.get("range")));
                    }
                    if (templateNode.has("phosphorus")) {
                        JsonNode phosphorusNode = templateNode.get("phosphorus");
                        generatedData.put("phosphorus_value", generateValueFromRange(phosphorusNode.get("range")));
                    }
                    if (templateNode.has("potassium")) {
                        JsonNode potassiumNode = templateNode.get("potassium");
                        generatedData.put("potassium_value", generateValueFromRange(potassiumNode.get("range")));
                    }
                    break;
                    
                case "soil_ph":
                    generatedData.put("value", generateValueFromRange(deviceTypeNode.get("range")));
                    break;
                    
                case "soil_trace_elements":
                    // 土壤微量元素包含多种元素
                    if (templateNode.has("iron")) {
                        JsonNode ironNode = templateNode.get("iron");
                        generatedData.put("iron_value", generateValueFromRange(ironNode.get("range")));
                    }
                    if (templateNode.has("zinc")) {
                        JsonNode zincNode = templateNode.get("zinc");
                        generatedData.put("zinc_value", generateValueFromRange(zincNode.get("range")));
                    }
                    if (templateNode.has("copper")) {
                        JsonNode copperNode = templateNode.get("copper");
                        generatedData.put("copper_value", generateValueFromRange(copperNode.get("range")));
                    }
                    if (templateNode.has("manganese")) {
                        JsonNode manganeseNode = templateNode.get("manganese");
                        generatedData.put("manganese_value", generateValueFromRange(manganeseNode.get("range")));
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
                    "LEFT JOIN device_template dt ON d.dt_id = dt.dt_id " +
                    "WHERE d.device_id = ?";
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            
            stmt.setInt(1, Integer.parseInt(deviceId));  // device_id是int类型
            ResultSet rs = stmt.executeQuery();
            
            if (rs.next()) {
                DeviceInfo info = new DeviceInfo();
                info.deviceId = String.valueOf(rs.getInt("device_id"));
                info.deviceName = rs.getString("device_name");
                info.enName = rs.getString("en_name");
                info.template = rs.getString("template");
                return info;
            }
            
        } catch (SQLException | NumberFormatException e) {
            logger.error("Error querying device info for device {}", deviceId, e);
        }
        
        return null;
    }
    
    public void shutdown() {
        // 停止所有设备的数据生成任务
        logger.info("Stopping all device data generation tasks...");
        for (String deviceId : deviceTasks.keySet()) {
            stopDataGenerationForDevice(deviceId);
        }
        
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
    
    /**
     * 获取当前正在运行的设备数量
     */
    public int getActiveDeviceCount() {
        return deviceTasks.size();
    }
    
    /**
     * 检查特定设备是否正在运行
     */
    public boolean isDeviceActive(String deviceId) {
        ScheduledFuture<?> task = deviceTasks.get(deviceId);
        return task != null && !task.isCancelled() && !task.isDone();
    }
    
    /**
     * 系统启动时初始化所有状态为1的设备
     * 从数据库查询所有state=1的设备，并为它们启动数据生成任务
     */
    public void initializeActiveDevices() {
        logger.info("Initializing active devices from database...");
        
        String sql = "SELECT d.device_id, d.device_name, d.state, dt.en_name, dt.template " +
                    "FROM device d " +
                    "LEFT JOIN device_template dt ON d.dt_id = dt.dt_id " +
                    "WHERE d.state = 1";
        
        try (Connection conn = databaseManager.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            int activeDeviceCount = 0;
            while (rs.next()) {
                String deviceId = String.valueOf(rs.getInt("device_id"));
                String deviceName = rs.getString("device_name");
                String enName = rs.getString("en_name");
                String template = rs.getString("template");
                
                logger.info("Found active device: ID={}, Name={}, Type={}", deviceId, deviceName, enName);
                
                // 创建设备信息对象
                DeviceInfo deviceInfo = new DeviceInfo();
                deviceInfo.deviceId = deviceId;
                deviceInfo.deviceName = deviceName;
                deviceInfo.enName = enName;
                deviceInfo.template = template;
                
                // 启动数据生成任务
                scheduleDataGeneration(deviceInfo);
                activeDeviceCount++;
            }
            
            logger.info("Successfully initialized {} active devices", activeDeviceCount);
            
        } catch (SQLException e) {
            logger.error("Error initializing active devices from database", e);
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