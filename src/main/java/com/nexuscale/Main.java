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
        // Configure Hadoop for Windows environment
        configureHadoopForWindows();
        
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
    
    /**
     * Configure Hadoop settings for Windows environment
     */
    private static void configureHadoopForWindows() {
        // 设置hadoop.home.dir系统属性绕过检查
        String hadoopHome = System.getenv("HADOOP_HOME");
        if (hadoopHome == null || hadoopHome.isEmpty()) {
            // 设置一个虚拟路径，绕过Hadoop的本地文件系统检查
            System.setProperty("hadoop.home.dir", "C:\\");
            logger.info("HADOOP_HOME not set, using virtual path to bypass check");
        } else {
            System.setProperty("hadoop.home.dir", hadoopHome);
            logger.info("Using HADOOP_HOME: {}", hadoopHome);
        }
        
        // 禁用Hadoop的本地库加载
        System.setProperty("java.library.path", "");
        
        // 设置Hadoop在Windows上的兼容性属性
        System.setProperty("hadoop.util.shell.command.timeout", "30000");
        System.setProperty("io.file.buffer.size", "65536");
        
        // 配置远程Hadoop集群连接
        configureRemoteHadoopCluster();
    }
    
    /**
     * Configure Hadoop to connect to remote cluster
     */
    private static void configureRemoteHadoopCluster() {
        // 基于您的集群配置设置Hadoop客户端连接参数
        System.setProperty("fs.defaultFS", "hdfs://192.168.56.10:9000");
        System.setProperty("yarn.resourcemanager.address", "192.168.56.10:8032");
        System.setProperty("yarn.resourcemanager.scheduler.address", "192.168.56.10:8030");
        System.setProperty("yarn.resourcemanager.resource-tracker.address", "192.168.56.10:8031");
        
        // HBase远程集群配置
        System.setProperty("hbase.zookeeper.quorum", "192.168.56.11,192.168.56.12,192.168.56.13");
        System.setProperty("hbase.zookeeper.property.clientPort", "2181");
        System.setProperty("hbase.master", "192.168.56.10:16000");
        System.setProperty("hbase.rootdir", "hdfs://192.168.56.10:9000/hbase");
        
        // 禁用本地模式，强制使用分布式模式
        System.setProperty("hbase.cluster.distributed", "true");
        
        logger.info("已配置Hadoop客户端连接到远程集群:");
        logger.info("  - HDFS NameNode: hdfs://192.168.56.10:9000");
        logger.info("  - YARN ResourceManager: 192.168.56.10:8032");
        logger.info("  - HBase ZooKeeper: 192.168.56.11,192.168.56.12,192.168.56.13:2181");
        logger.info("  - HBase Master: 192.168.56.10:16000");
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