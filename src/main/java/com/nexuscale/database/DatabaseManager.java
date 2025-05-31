package com.nexuscale.database;

import com.nexuscale.config.ConfigManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class DatabaseManager {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseManager.class);
    
    private final String url;
    private final String username;
    private final String password;
    
    public DatabaseManager() {
        this.url = ConfigManager.getProperty("mysql.url");
        this.username = ConfigManager.getProperty("mysql.username");
        this.password = ConfigManager.getProperty("mysql.password");
        
        // Load MySQL driver
        try {
            Class.forName(ConfigManager.getProperty("mysql.driver"));
            logger.info("MySQL driver loaded successfully");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load MySQL driver", e);
        }
    }
    
    public Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, username, password);
    }
    
    public List<String> getDeviceTypes() {
        List<String> deviceTypes = new ArrayList<>();
        String sql = "SELECT DISTINCT en_name FROM device_template WHERE en_name IS NOT NULL AND en_name != ''";
        
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql);
             ResultSet rs = stmt.executeQuery()) {
            
            while (rs.next()) {
                String enName = rs.getString("en_name");
                deviceTypes.add(enName);
                logger.info("Found device type: {}", enName);
            }
            
        } catch (SQLException e) {
            logger.error("Error querying device types", e);
            throw new RuntimeException("Failed to query device types", e);
        }
        
        logger.info("Total device types found: {}", deviceTypes.size());
        return deviceTypes;
    }
    
    public boolean testConnection() {
        try (Connection conn = getConnection()) {
            return conn.isValid(5);
        } catch (SQLException e) {
            logger.error("Database connection test failed", e);
            return false;
        }
    }
} 