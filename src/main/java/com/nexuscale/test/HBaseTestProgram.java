package com.nexuscale.test;

import com.nexuscale.database.DatabaseManager;
import com.nexuscale.hbase.HBaseManager;
import com.nexuscale.kafka.KafkaProducerManager;
import com.nexuscale.service.SensorDataGeneratorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class HBaseTestProgram {
    private static final Logger logger = LoggerFactory.getLogger(HBaseTestProgram.class);
    
    public static void main(String[] args) {
        logger.info("=== HBase IoT Sensor Data Test Program ===");
        
        // 初始化组件
        DatabaseManager databaseManager = null;
        HBaseManager hbaseManager = null;
        KafkaProducerManager kafkaProducer = null;
        SensorDataGeneratorService sensorDataGenerator = null;
        
        try {
            databaseManager = new DatabaseManager();
            hbaseManager = new HBaseManager();
            kafkaProducer = new KafkaProducerManager();
            sensorDataGenerator = new SensorDataGeneratorService(databaseManager, hbaseManager, kafkaProducer);
            
            // 测试连接
            if (!testConnections(databaseManager, hbaseManager, kafkaProducer)) {
                logger.error("Connection tests failed. Exiting...");
                return;
            }
            
            // 获取设备类型并准备HBase列
            setupHBaseColumns(databaseManager, hbaseManager);
            
            // 运行测试菜单
            runTestMenu(hbaseManager, sensorDataGenerator);
            
        } catch (Exception e) {
            logger.error("Error in test program", e);
        } finally {
            // 清理资源
            cleanup(hbaseManager, kafkaProducer, sensorDataGenerator);
        }
    }
    
    private static boolean testConnections(DatabaseManager databaseManager, 
                                         HBaseManager hbaseManager, 
                                         KafkaProducerManager kafkaProducer) {
        logger.info("Testing connections...");
        
        if (!databaseManager.testConnection()) {
            logger.error("MySQL connection failed");
            return false;
        }
        
        if (!hbaseManager.testConnection()) {
            logger.error("HBase connection failed");
            return false;
        }
        
        if (!kafkaProducer.testConnection()) {
            logger.error("Kafka connection failed");
            return false;
        }
        
        logger.info("All connections successful!");
        return true;
    }
    
    private static void setupHBaseColumns(DatabaseManager databaseManager, HBaseManager hbaseManager) {
        logger.info("Setting up HBase columns...");
        
        try {
            List<String> deviceTypes = databaseManager.getDeviceTypes();
            hbaseManager.createColumnsIfNotExists(deviceTypes);
            
            logger.info("HBase columns setup completed for device types: {}", deviceTypes);
        } catch (Exception e) {
            logger.error("Error setting up HBase columns", e);
        }
    }
    
    private static void runTestMenu(HBaseManager hbaseManager, SensorDataGeneratorService sensorDataGenerator) {
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        
        while (running) {
            printMenu();
            System.out.print("请选择操作: ");
            
            try {
                int choice = Integer.parseInt(scanner.nextLine().trim());
                
                switch (choice) {
                    case 1:
                        testInsertSampleData(hbaseManager);
                        break;
                    case 2:
                        testQueryData(hbaseManager, scanner);
                        break;
                    case 3:
                        testScanDeviceData(hbaseManager, scanner);
                        break;
                    case 4:
                        testGenerateSensorData(sensorDataGenerator, scanner);
                        break;
                    case 5:
                        testBatchInsert(hbaseManager);
                        break;
                    case 0:
                        running = false;
                        break;
                    default:
                        System.out.println("无效选择，请重新输入！");
                }
                
            } catch (NumberFormatException e) {
                System.out.println("请输入有效的数字！");
            } catch (Exception e) {
                logger.error("Error executing test operation", e);
                System.out.println("操作执行失败，请查看日志！");
            }
        }
        
        scanner.close();
    }
    
    private static void printMenu() {
        System.out.println("\n=== HBase IoT 传感器数据测试菜单 ===");
        System.out.println("1. 插入示例传感器数据");
        System.out.println("2. 查询指定设备数据");
        System.out.println("3. 扫描设备历史数据");
        System.out.println("4. 测试传感器数据生成");
        System.out.println("5. 批量插入测试数据");
        System.out.println("0. 退出程序");
        System.out.println("=====================================");
    }
    
    private static void testInsertSampleData(HBaseManager hbaseManager) {
        logger.info("Inserting sample sensor data...");
        
        // 插入温度传感器数据 (设备ID: 13)
        Map<String, String> tempData = new HashMap<>();
        tempData.put("temperature", "25.3");
        tempData.put("unit", "°C");
        tempData.put("battery_level", "87");
        tempData.put("signal_strength", "-45");
        
        long timestamp1 = System.currentTimeMillis();
        hbaseManager.putSensorData("13", timestamp1, tempData);
        
        // 插入湿度传感器数据 (设备ID: 14)
        Map<String, String> humidityData = new HashMap<>();
        humidityData.put("humidity", "65.2");
        humidityData.put("unit", "%RH");
        humidityData.put("battery_level", "92");
        humidityData.put("signal_strength", "-38");
        
        long timestamp2 = System.currentTimeMillis() + 1000;
        hbaseManager.putSensorData("14", timestamp2, humidityData);
        
        // 插入空气成分传感器数据 (假设设备ID: 15)
        Map<String, String> airData = new HashMap<>();
        airData.put("co2", "850");
        airData.put("o2", "20.9");
        airData.put("pm25", "35");
        airData.put("co2_unit", "ppm");
        airData.put("o2_unit", "%");
        airData.put("pm25_unit", "μg/m³");
        airData.put("battery_level", "78");
        airData.put("signal_strength", "-52");
        
        long timestamp3 = System.currentTimeMillis() + 2000;
        hbaseManager.putSensorData("15", timestamp3, airData);
        
        // 插入土壤NPK传感器数据 (设备ID: 10)
        Map<String, String> npkData = new HashMap<>();
        npkData.put("nitrogen", "120");
        npkData.put("phosphorus", "45");
        npkData.put("potassium", "180");
        npkData.put("unit", "mg/kg");
        npkData.put("battery_level", "95");
        npkData.put("signal_strength", "-42");
        
        long timestamp4 = System.currentTimeMillis() + 3000;
        hbaseManager.putSensorData("10", timestamp4, npkData);
        
        System.out.println("示例数据插入完成！");
        System.out.println("- 设备13: 温度传感器数据 (时间戳: " + timestamp1 + ")");
        System.out.println("- 设备14: 湿度传感器数据 (时间戳: " + timestamp2 + ")");
        System.out.println("- 设备15: 空气成分传感器数据 (时间戳: " + timestamp3 + ")");
        System.out.println("- 设备10: 土壤NPK传感器数据 (时间戳: " + timestamp4 + ")");
    }
    
    private static void testQueryData(HBaseManager hbaseManager, Scanner scanner) {
        System.out.print("请输入设备ID: ");
        String deviceId = scanner.nextLine().trim();
        
        System.out.print("请输入时间戳 (留空使用当前时间): ");
        String timestampStr = scanner.nextLine().trim();
        
        long timestamp;
        if (timestampStr.isEmpty()) {
            timestamp = System.currentTimeMillis();
        } else {
            try {
                timestamp = Long.parseLong(timestampStr);
            } catch (NumberFormatException e) {
                System.out.println("无效的时间戳格式！");
                return;
            }
        }
        
        Map<String, String> data = hbaseManager.getSensorData(deviceId, timestamp);
        
        if (data != null && !data.isEmpty()) {
            System.out.println("查询结果:");
            System.out.println("设备ID: " + deviceId);
            System.out.println("时间戳: " + timestamp);
            for (Map.Entry<String, String> entry : data.entrySet()) {
                System.out.println("  " + entry.getKey() + ": " + entry.getValue());
            }
        } else {
            System.out.println("未找到指定设备和时间戳的数据！");
        }
    }
    
    private static void testScanDeviceData(HBaseManager hbaseManager, Scanner scanner) {
        System.out.print("请输入设备ID: ");
        String deviceId = scanner.nextLine().trim();
        
        System.out.print("请输入查询条数限制 (留空默认10条): ");
        String limitStr = scanner.nextLine().trim();
        
        int limit = 10;
        if (!limitStr.isEmpty()) {
            try {
                limit = Integer.parseInt(limitStr);
            } catch (NumberFormatException e) {
                System.out.println("无效的数字格式，使用默认值10");
            }
        }
        
        System.out.println("扫描设备 " + deviceId + " 的历史数据 (最多" + limit + "条):");
        hbaseManager.scanDeviceData(deviceId, limit);
    }
    
    private static void testGenerateSensorData(SensorDataGeneratorService sensorDataGenerator, Scanner scanner) {
        System.out.print("请输入设备ID: ");
        String deviceId = scanner.nextLine().trim();
        
        System.out.println("开始为设备 " + deviceId + " 生成传感器数据...");
        sensorDataGenerator.startDataGeneration(deviceId, 1);
        
        System.out.println("数据生成已启动！数据将每10秒生成一次并存储到HBase和Kafka。");
        System.out.println("按Enter键停止生成...");
        scanner.nextLine();
        
        System.out.println("数据生成测试完成！");
    }
    
    private static void testBatchInsert(HBaseManager hbaseManager) {
        System.out.println("开始批量插入测试数据...");
        
        String[] deviceIds = {"13", "14", "10", "15", "16"};
        String[] deviceTypes = {"temperature", "humidity", "soil_npk", "air_component", "light_intensity"};
        
        for (int i = 0; i < deviceIds.length; i++) {
            for (int j = 0; j < 5; j++) {
                Map<String, String> data = generateSampleData(deviceTypes[i], j);
                long timestamp = System.currentTimeMillis() + (i * 5000) + (j * 1000);
                
                hbaseManager.putSensorData(deviceIds[i], timestamp, data);
                
                try {
                    Thread.sleep(100); // 避免时间戳冲突
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
        
        System.out.println("批量插入完成！已为5个设备各插入5条测试数据。");
    }
    
    private static Map<String, String> generateSampleData(String deviceType, int index) {
        Map<String, String> data = new HashMap<>();
        
        switch (deviceType.toLowerCase()) {
            case "temperature":
                data.put("temperature", String.format("%.1f", 20.0 + index * 2));
                data.put("unit", "°C");
                break;
            case "humidity":
                data.put("humidity", String.format("%.1f", 40.0 + index * 5));
                data.put("unit", "%RH");
                break;
            case "soil_npk":
                data.put("nitrogen", String.format("%.0f", 100.0 + index * 20));
                data.put("phosphorus", String.format("%.0f", 30.0 + index * 10));
                data.put("potassium", String.format("%.0f", 150.0 + index * 30));
                data.put("unit", "mg/kg");
                break;
            case "air_component":
                data.put("co2", String.format("%.0f", 400.0 + index * 100));
                data.put("o2", String.format("%.1f", 20.0 + index * 0.5));
                data.put("pm25", String.format("%.0f", 10.0 + index * 10));
                data.put("co2_unit", "ppm");
                data.put("o2_unit", "%");
                data.put("pm25_unit", "μg/m³");
                break;
            case "light_intensity":
                data.put("illuminance", String.format("%.0f", 1000.0 + index * 5000));
                data.put("unit", "lux");
                break;
        }
        
        data.put("battery_level", String.valueOf(80 + index * 3));
        data.put("signal_strength", String.valueOf(-40 - index * 2));
        data.put("device_type", deviceType);
        
        return data;
    }
    
    private static void cleanup(HBaseManager hbaseManager, 
                              KafkaProducerManager kafkaProducer,
                              SensorDataGeneratorService sensorDataGenerator) {
        logger.info("Cleaning up resources...");
        
        if (sensorDataGenerator != null) {
            sensorDataGenerator.shutdown();
        }
        
        if (hbaseManager != null) {
            hbaseManager.close();
        }
        
        if (kafkaProducer != null) {
            kafkaProducer.close();
        }
        
        logger.info("Cleanup completed");
    }
} 