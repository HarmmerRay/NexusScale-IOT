package com.nexuscale.hbase;

import com.nexuscale.config.ConfigManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class HBaseManager {
    private static final Logger logger = LoggerFactory.getLogger(HBaseManager.class);
    
    private Connection connection;
    private final String tableName;
    private final String columnFamily;
    
    public HBaseManager() {
        this.tableName = ConfigManager.getProperty("hbase.table.name");
        this.columnFamily = ConfigManager.getProperty("hbase.column.family");
        initializeConnection();
    }
    
    private void initializeConnection() {
        try {
            Configuration config = HBaseConfiguration.create();
            
            // 设置ZooKeeper连接信息
            config.set("hbase.zookeeper.quorum", ConfigManager.getProperty("hbase.zookeeper.quorum"));
            config.set("hbase.zookeeper.property.clientPort", ConfigManager.getProperty("hbase.zookeeper.property.clientPort"));
            
            // 针对Windows客户端连接远程HBase集群的配置
            config.set("hbase.client.retries.number", "3");
            config.set("hbase.client.pause", "1000");
            config.set("hbase.rpc.timeout", "60000");
            config.set("hbase.client.operation.timeout", "120000");
            config.set("hbase.client.scanner.timeout.period", "60000");
            
            // 禁用安全相关的配置（适用于测试环境）
            config.set("hadoop.security.authentication", "simple");
            config.set("hbase.security.authentication", "simple");
            
            // 设置用户信息
            System.setProperty("HADOOP_USER_NAME", "root");
            
            this.connection = ConnectionFactory.createConnection(config);
            logger.info("HBase connection established successfully");
        } catch (IOException e) {
            logger.error("Failed to establish HBase connection", e);
            throw new RuntimeException("HBase connection failed", e);
        }
    }
    
    public boolean testConnection() {
        try {
            Admin admin = connection.getAdmin();
            boolean exists = admin.tableExists(TableName.valueOf(tableName));
            admin.close();
            logger.info("HBase connection test successful. Table {} exists: {}", tableName, exists);
            return true;
        } catch (Exception e) {
            logger.error("HBase connection test failed", e);
            return false;
        }
    }
    
    public void createColumnsIfNotExists(List<String> columnNames) {
        try {
            Admin admin = connection.getAdmin();
            TableName tableNameObj = TableName.valueOf(tableName);
            
            if (!admin.tableExists(tableNameObj)) {
                logger.error("Table {} does not exist", tableName);
                admin.close();
                return;
            }
            
            // HBase中列是动态的，不需要预先创建列，这里只是记录日志
            logger.info("Columns to be used in table {}: {}", tableName, columnNames);
            admin.close();
            
        } catch (IOException e) {
            logger.error("Error checking table columns", e);
        }
    }
    
    public void putSensorData(String deviceId, long timestamp, Map<String, String> sensorData) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            
            String rowKey = deviceId + "_" + timestamp;
            Put put = new Put(Bytes.toBytes(rowKey));
            
            // 只添加传感器数据，不添加timestamp和device_id（它们已经在行键中体现）
            for (Map.Entry<String, String> entry : sensorData.entrySet()) {
                put.addColumn(Bytes.toBytes(columnFamily), 
                            Bytes.toBytes(entry.getKey()), 
                            Bytes.toBytes(entry.getValue()));
            }
            
            table.put(put);
            table.close();
            
            logger.info("Successfully stored sensor data for device {} at timestamp {}", deviceId, timestamp);
            
        } catch (IOException e) {
            logger.error("Failed to store sensor data for device {}", deviceId, e);
            throw new RuntimeException("HBase put operation failed", e);
        }
    }
    
    public Map<String, String> getSensorData(String deviceId, long timestamp) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            
            String rowKey = deviceId + "_" + timestamp;
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);
            
            if (result.isEmpty()) {
                logger.warn("No data found for device {} at timestamp {}", deviceId, timestamp);
                table.close();
                return null;
            }
            
            Map<String, String> data = new java.util.HashMap<>();
            result.getFamilyMap(Bytes.toBytes(columnFamily)).forEach((qualifier, value) -> {
                data.put(Bytes.toString(qualifier), Bytes.toString(value));
            });
            
            table.close();
            logger.info("Retrieved sensor data for device {} at timestamp {}: {} fields", 
                       deviceId, timestamp, data.size());
            
            return data;
            
        } catch (IOException e) {
            logger.error("Failed to retrieve sensor data for device {}", deviceId, e);
            throw new RuntimeException("HBase get operation failed", e);
        }
    }
    
    public void scanDeviceData(String deviceId, int limit) {
        try {
            Table table = connection.getTable(TableName.valueOf(tableName));
            
            Scan scan = new Scan();
            scan.setRowPrefixFilter(Bytes.toBytes(deviceId + "_"));
            if (limit > 0) {
                scan.setLimit(limit);
            }
            
            ResultScanner scanner = table.getScanner(scan);
            
            logger.info("=== Scanning data for device: {} ===", deviceId);
            int count = 0;
            for (Result result : scanner) {
                count++;
                String rowKey = Bytes.toString(result.getRow());
                logger.info("Row Key: {}", rowKey);
                
                Map<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily));
                for (Map.Entry<byte[], byte[]> entry : familyMap.entrySet()) {
                    String qualifier = Bytes.toString(entry.getKey());
                    String value = Bytes.toString(entry.getValue());
                    logger.info("  {}: {}", qualifier, value);
                }
                logger.info("---");
            }
            
            scanner.close();
            table.close();
            
            logger.info("=== Scan completed. Total records found: {} ===", count);
            
        } catch (IOException e) {
            logger.error("Failed to scan data for device {}", deviceId, e);
        }
    }
    
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                logger.info("HBase connection closed");
            }
        } catch (IOException e) {
            logger.error("Error closing HBase connection", e);
        }
    }
} 