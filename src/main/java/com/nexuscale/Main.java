package com.nexuscale;

import com.nexuscale.service.DeviceDataConsumerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger logger = LoggerFactory.getLogger(Main.class);
    private static DeviceDataConsumerService consumerService;
    private static ScheduledExecutorService statusScheduler;
    
    public static void main(String[] args) {
        logger.info("=== NexusScale IoT Device Data Consumer ===");
        logger.info("Starting application...");
        
        try {
            // Initialize and start the consumer service
            consumerService = new DeviceDataConsumerService();
            consumerService.start();
            
            // Set up graceful shutdown
            setupShutdownHook();
            
            // Schedule periodic status reports
            setupStatusReporting();
            
            // Keep the application running and provide interactive commands
            runInteractiveMode();
            
        } catch (Exception e) {
            logger.error("Fatal error occurred", e);
            System.exit(1);
        }
    }
    
    private static void setupShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown signal received, stopping services...");
            
            if (statusScheduler != null && !statusScheduler.isShutdown()) {
                statusScheduler.shutdown();
            }
            
            if (consumerService != null) {
                consumerService.shutdown();
            }
            
            logger.info("Application shutdown completed");
        }));
    }
    
    private static void setupStatusReporting() {
        statusScheduler = Executors.newSingleThreadScheduledExecutor();
        statusScheduler.scheduleAtFixedRate(() -> {
            if (consumerService != null) {
                consumerService.printStatus();
            }
        }, 30, 30, TimeUnit.SECONDS);
    }
    
    private static void runInteractiveMode() {
        logger.info("Application started successfully. Type 'help' for available commands.");
        
        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        
        while (running) {
            System.out.print("\nnexus-iot> ");
            String input = scanner.nextLine().trim().toLowerCase();
            
            switch (input) {
                case "help":
                    printHelp();
                    break;
                case "status":
                    if (consumerService != null) {
                        consumerService.printStatus();
                    }
                    break;
                case "refresh":
                    if (consumerService != null) {
                        consumerService.refreshDeviceTypes();
                        logger.info("Device types refreshed");
                    }
                    break;
                case "quit":
                case "exit":
                    logger.info("Exiting application...");
                    running = false;
                    break;
                default:
                    if (!input.isEmpty()) {
                        System.out.println("Unknown command: " + input + ". Type 'help' for available commands.");
                    }
                    break;
            }
        }
        
        scanner.close();
        System.exit(0);
    }
    
    private static void printHelp() {
        System.out.println("\n=== Available Commands ===");
        System.out.println("help     - Show this help message");
        System.out.println("status   - Show current consumer status");
        System.out.println("refresh  - Refresh device types from database");
        System.out.println("quit     - Exit the application");
        System.out.println("exit     - Exit the application");
        System.out.println("========================\n");
    }
}