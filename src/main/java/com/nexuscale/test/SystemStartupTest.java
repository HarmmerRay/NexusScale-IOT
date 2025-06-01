package com.nexuscale.test;

import com.nexuscale.service.DeviceDataConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;

public class SystemStartupTest {
    private static final Logger logger = LoggerFactory.getLogger(SystemStartupTest.class);
    
    public static void main(String[] args) {
        logger.info("=== IoT System Startup Test ===");
        
        DeviceDataConsumerService service = new DeviceDataConsumerService();
        
        // 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down system...");
            service.shutdown();
        }));
        
        try {
            // 启动服务
            service.start();
            
            // 打印状态
            service.printStatus();
            
            logger.info("System started successfully!");
            logger.info("The system will:");
            logger.info("1. Initialize all devices with state=1 from database");
            logger.info("2. Start generating sensor data for active devices");
            logger.info("3. Monitor Redis queues for device state messages");
            logger.info("4. Start/stop device data generation based on Redis messages");
            logger.info("");
            logger.info("Press 'q' to quit, 's' to show status:");
            
            Scanner scanner = new Scanner(System.in);
            String input;
            
            while (!(input = scanner.nextLine().trim().toLowerCase()).equals("q")) {
                switch (input) {
                    case "s":
                    case "status":
                        service.printStatus();
                        break;
                    case "refresh":
                        service.refreshDeviceTypes();
                        break;
                    case "help":
                    case "h":
                        printHelp();
                        break;
                    default:
                        logger.info("Unknown command: {}. Type 'help' for available commands.", input);
                        break;
                }
            }
            
        } catch (Exception e) {
            logger.error("Error starting system", e);
        } finally {
            service.shutdown();
            logger.info("System shutdown completed");
        }
    }
    
    private static void printHelp() {
        logger.info("Available commands:");
        logger.info("  s, status  - Show system status");
        logger.info("  refresh    - Refresh device types from database");
        logger.info("  h, help    - Show this help message");
        logger.info("  q          - Quit the system");
    }
} 