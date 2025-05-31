# NexusScale IoT Device Data Consumer

这是一个基于Java的IoT设备数据消费系统，用于从MySQL数据库查询设备类型，并为每个设备类型创建Redis消费者来监控和处理实时数据。

## 功能特性

- **自动设备类型发现**: 从MySQL数据库的`device_template`表查询所有设备类型（`en_name`字段）
- **多线程Redis消费**: 为每个设备类型创建独立的Redis消费者线程
- **实时数据处理**: 使用Redis阻塞操作（BLPOP）实时消费数据
- **连接池管理**: 高效的Redis连接池管理
- **优雅关闭**: 支持优雅的应用程序关闭
- **交互式命令**: 提供命令行界面进行运行时操作
- **状态监控**: 定期打印消费者状态信息
- **日志记录**: 完整的日志记录系统

## 系统架构

```
MySQL Database (device_template表)
    ↓ 查询en_name字段
DeviceDataConsumerService
    ↓ 为每个设备类型创建
RedisConsumer (多线程)
    ↓ 消费Redis List数据
数据处理和打印
```

## 项目结构

```
src/main/java/com/nexuscale/
├── Main.java                           # 主入口类
├── config/
│   └── ConfigManager.java              # 配置管理
├── database/
│   └── DatabaseManager.java            # MySQL数据库管理
├── redis/
│   └── RedisManager.java               # Redis连接管理
├── consumer/
│   └── RedisConsumer.java              # Redis消费者
└── service/
    └── DeviceDataConsumerService.java  # 核心服务类

src/main/resources/
├── application.properties              # 应用配置
└── logback.xml                        # 日志配置
```

## 环境要求

- Java 8+
- MySQL 8.0+
- Redis 6.0+
- Maven 3.6+ (可选，也可以手动编译)

## 配置说明

### 数据库配置 (application.properties)

```properties
# MySQL配置
mysql.url=jdbc:mysql://localhost:3306/nexuscale_iot?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true
mysql.username=root
mysql.password=password

# Redis配置
redis.host=localhost
redis.port=6379
redis.database=0

# 应用配置
app.consumer.threads=10        # 消费者线程池大小
app.polling.interval=1000      # 轮询间隔(毫秒)
```

### 数据库表结构

确保MySQL数据库中存在`device_template`表，包含`en_name`字段：

```sql
CREATE TABLE device_template (
    id INT PRIMARY KEY AUTO_INCREMENT,
    en_name VARCHAR(255) NOT NULL,
    -- 其他字段...
);

-- 示例数据
INSERT INTO device_template (en_name) VALUES 
('temperature_sensor'),
('humidity_sensor'),
('motion_detector'),
('smart_camera');
```

## 安装和运行

### 1. 克隆项目
```bash
git clone <repository-url>
cd NexusScale-IOT
```

### 2. 配置数据库和Redis
- 确保MySQL和Redis服务正在运行
- 修改`src/main/resources/application.properties`中的连接配置

### 3. 编译项目

#### 使用Maven (推荐)
```bash
mvn clean compile
```

#### 手动编译 (如果没有Maven)
1. 下载所需的JAR依赖文件到 `target/lib/` 目录：
   - mysql-connector-java-8.0.33.jar
   - jedis-4.4.3.jar
   - jackson-databind-2.15.2.jar
   - jackson-core-2.15.2.jar
   - jackson-annotations-2.15.2.jar
   - slf4j-api-1.7.36.jar
   - logback-classic-1.2.12.jar
   - logback-core-1.2.12.jar

2. 运行编译脚本：
```bash
compile.bat
```

### 4. 运行应用

#### 使用Maven
```bash
mvn exec:java -Dexec.mainClass="com.nexuscale.Main"
```

#### 手动运行
```bash
run.bat
```

或者直接使用java命令：
```bash
java -cp "target/lib/*;target/classes" com.nexuscale.Main
```

## 使用说明

### 交互式命令

应用启动后，可以使用以下命令：

- `help` - 显示帮助信息
- `status` - 显示当前消费者状态
- `refresh` - 从数据库刷新设备类型
- `quit` / `exit` - 退出应用

### 数据格式

Redis中的数据通过`RPUSH`命令推送到以设备类型命名的列表中：

```bash
# 示例：向temperature_sensor队列推送数据
redis-cli RPUSH temperature_sensor '{"deviceId":"device001","temperature":23.5,"timestamp":"2024-01-15T10:30:00Z"}'
redis-cli RPUSH humidity_sensor '{"deviceId":"device002","humidity":65.2,"timestamp":"2024-01-15T10:30:05Z"}'
```

### 日志输出

消费到的数据会以以下格式打印：

```
[2024-01-15 10:30:00] Topic: temperature_sensor | Data: {"deviceId":"device001","temperature":23.5,"timestamp":"2024-01-15T10:30:00Z"}
```

## 监控和维护

### 状态监控
- 应用每30秒自动打印状态信息
- 使用`status`命令手动查看状态

### 日志文件
- 控制台输出：实时查看
- 文件日志：`logs/nexuscale-iot.log`
- 日志文件自动滚动，保留30天历史

### 优雅关闭
- 使用`Ctrl+C`或`quit`命令安全关闭应用
- 所有消费者线程会被优雅关闭
- Redis连接池会被正确释放

## 扩展和定制

### 添加数据处理逻辑
在`RedisConsumer.processMessage()`方法中添加自定义处理逻辑：

```java
private void processMessage(String data) {
    // 记录日志
    String timestamp = LocalDateTime.now().format(FORMATTER);
    logger.info("[{}] Topic: {} | Data: {}", timestamp, topic, data);
    
    // 添加自定义处理逻辑
    try {
        // JSON解析
        ObjectMapper mapper = new ObjectMapper();
        JsonNode jsonNode = mapper.readTree(data);
        
        // 数据验证
        if (validateData(jsonNode)) {
            // 业务处理
            processBusinessLogic(jsonNode);
        }
    } catch (Exception e) {
        logger.error("Error processing message: {}", data, e);
    }
}
```

### 性能调优
- 调整`app.consumer.threads`参数控制并发度
- 调整`app.polling.interval`参数控制轮询频率
- 调整Redis连接池参数

## 故障排除

### 常见问题

1. **数据库连接失败**
   - 检查MySQL服务状态
   - 验证连接参数和权限

2. **Redis连接失败**
   - 检查Redis服务状态
   - 验证网络连接和端口

3. **没有找到设备类型**
   - 检查`device_template`表是否存在
   - 验证`en_name`字段数据

4. **消费者没有接收到数据**
   - 确认Redis队列名称与数据库中的`en_name`匹配
   - 检查数据是否正确推送到Redis

## 许可证

[License Information]

## 贡献

欢迎提交Issue和Pull Request来改进此项目。 