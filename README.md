# NexusScale IoT Device Data Consumer

这是一个基于Java的IoT设备数据消费系统，用于从MySQL数据库查询设备类型，并为每个设备类型创建Redis消费者来监控和处理实时数据。
## 数据存储 hadoop集群服务器配置信息

### Hadoop配置
格式化：hdfs namenode -format
如果修改了配置文件需要重新格式化，记得将所有机器上的datanode和namenode、tmp目录里面的东西都给删除，再在master机器上执行格式化命令。否则会报，clusterId不匹配。

测试： echo "hello world!" > input.txt中，创建这样一个文本文件
hdfs dfs -mkdir -p /test
hdfs dfs -put input.txt /test
hadoop jar hadoop-mapreduce-examples-3.1.3.jar wordcount /test /test/output

### Hbase配置

### Hive配置


### Prometheus+Grafana配置

准备包: prometheus-3.2.1.linux-amd64.tar.gz   grafana-enterprise-11.3.3.linux-amd64.tar.gz

​	node_exporter-1.9.0.linux-amd64.tar.gz

四台虚拟机上 :

```
tar -zxvf node_exporter-1.9.0.linux-amd64.tar.gz

cd node_exporter-1.9.0.linux-amd64

node_exporter &
```
Master机器上:

tar -zxvf prometheus-3.2.1.linux-amd64

cd prometheus-3.2.1.linux-amd64

vim prometheus.yml  修改文件内容scrape_configs，添加一个job_name，告知其集群的信息

```
scrape_configs:
  # The job name is added as a label `job=<job_name>` to any timeseries scraped from this config.
  - job_name: "prometheus"

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
      - targets: ["master:9090"]
  - job_name: 'node_exporter'
    static_configs:
      - targets: ['slave1:9100','slave2:9100','slave3:9100']
```

./prometheus --config.file=prometheus.yml &

tar -zxvf grafana-enterprise-11.3.3.linux-amd64.tar.gz

cd grafana-enterprise-11.3.3.linux-amd64

grafana-server web &

## hadoop集群各组件启动信息
四台虚拟机ip地址分别为 192.168.56.10 192.168.56.11 192.168.56.12 192.168.56.13
各组件版本：  
hadoop-3.1.3  
zookeeper-3.4.10 √    zookeeper-3.5.6(错错错 大bug! 不知道的异常！)  
kafka_2.13-3.0.0  
hbase-2.2.2  
hive-3.1.2  
jdk1.8.0_201  

### 启动hadoop
master单机执行 start-dfs.sh ,start-yarn.sh
访问hadoop: 192.168.56.10:9870
访问yarn: 192.168.56.10:8088

### 启动ZooKeeper(只在三台slave上部署)

三台slave机器执行 zkServer.sh start
zkServer.sh status 查看启动状态
jps会看到 QuorumPeerMain   且日志目录没有任何文件 就是没有报错

然后清除所有旧文件
# 连接到ZooKeeper清理HBase节点
zkCli.sh

ls /
rmr /hbase

rmr /brokers
rmr /admin
rmr /isr_change_notification  
rmr /consumers
rmr /config
rmr /controller_epoch
rmr /cluster
rmr /feature
rmr /latest_producer_id_block
rmr /log_dir_event_notification
quit

# 停止HBase集群
stop-hbase.sh

# 确认所有HBase进程都已停止
jps | grep -E "(HMaster|HRegionServer)"

# 如果还有进程，强制杀掉
pkill -f HMaster
pkill -f HRegionServer

# 删除HDFS中的整个HBase目录
hdfs dfs -rm -r -f /hbase

# 确认删除成功
hdfs dfs -ls /

# 删除本地HBase临时数据（根据你的HBase配置调整路径）
rm -rf /tmp/hbase-*
rm -rf /opt/hbase/logs/*

# 删除Kafka数据目录（在每个节点上执行）
rm -rf /tmp/kafka-logs/*

# 也要删除meta.properties文件
sudo find /tmp -name "meta.properties" -delete

### 启动hbase(先启动ZK)
!!!! hbase问题很多!!!!  
再次启动时候一定要把所有的关于hbase的数据全部删除 重新建表  

以上在hdfs 本地文件系统  zookeeper中删除彻底 再执行下面启动命令  

master单机执行 start-hbase.sh
hbase shell
list
create 'iot_sensor_data','cf1'
访问hbase: 192.168.56.10:16010

### 启动Kafka(只在三台slave上，无Kraft)
！！！很多问题！！！  
再次启动时候  清理所有Kafka相关旧数据

清楚完毕再执行启动
三台slave机器执行：进入到kafka_2.13-3.0.0:目录下执行
kafka-server-start.sh -daemon config/server.properties
启动成功后JPS会显示Kafka   停止kafka kafka-server-stop.sh
测试：
创建topic(在一台机器上执行即可):
kafka-topics.sh --create --bootstrap-server slave1:9092,slave2:9092,slave3:9092 --topic topicName --partitions 3 --replication-factor 2

列出topic：
kafka-topics.sh --list --bootstrap-server slave1:9092,slave2:9092,slave3:9092

### 启动hive(依赖mysql)

配置好后执行此命令初始化一次（只用首次启动时）：schematool -dbType mysql -initSchema
终端连接：弄好后 hive 即可使用hive-cli连接上hive了  
服务端启动：hive --service hiveserver2 &  （允许使用jdbc访问hive）  
使用beeline -u "jdbc:hive2://localhost:10000" -n root -p 111111

### 启动Prometheus+Grafana

只在master机器上 ：   
1、./node_exporter &  所有机器上执行  进入到node文件夹底下  
2、./prometheus --config.file=prometheus.yml &  prometheus文件夹里面  只在master机器上
3、grafana-server web & 进入grafana bin里面  只在master机器上

浏览器访问http://192.168.56.10:3000 admin 111111

### 启动ElasticSearch+Kibana
四台机器 cd elastisearch/bin     ./elasticsearch
一台机器 cd kibana/bin ./kibana

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

### 准备工作

1. **初始化数据库**
```bash
# 在MySQL中运行database_setup.sql脚本
mysql -u root -p nexuscale_iot < database_setup.sql
```

2. **确保HBase表存在**
```bash
# 在HBase shell中创建表
hbase shell
create 'iot_sensor_data', 'cf1'
```

3. **创建Kafka主题**
```bash
# 在Kafka集群中创建主题
kafka-topics.sh --create --bootstrap-server 192.168.56.11:9092,192.168.56.12:9092,192.168.56.13:9092 --topic sensor_data_topic --partitions 3 --replication-factor 2
```

### 运行方式

重新编译
mvn clean compile "-Dmaven.compiler.source=1.8" "-Dmaven.compiler.target=1.8"

#### 1. 运行主应用程序（Redis消费者服务）
```bash
mvn clean compile exec:java -Dexec.mainClass="com.nexuscale.Main"
```

#### 2. 运行HBase测试程序
```bash
mvn clean compile exec:java -Dexec.mainClass="com.nexuscale.test.HBaseTestProgram"
```

或使用脚本：
```bash
run_test.bat
```

### 主应用程序交互式命令

应用启动后，可以使用以下命令：

- `help` - 显示帮助信息
- `status` - 显示当前消费者状态
- `refresh` - 从数据库刷新设备类型
- `quit` / `exit` - 退出应用

### 数据处理流程

1. **设备状态消息触发**：向Redis队列推送设备状态消息
```bash
# 开启设备14
redis-cli RPUSH temperature '{"deviceId":"14","state":1}'
redis-cli RPUSH humidity '{"deviceId":"13","state":1}'
```

2. **自动数据生成**：系统检测到设备开启后，自动生成相应类型的传感器数据

3. **数据存储**：
   - 发送到Kafka集群的`sensor_data_topic`主题
   - 存储到HBase的`iot_sensor_data`表中

4. **数据格式**：
   - **行键**：`[deviceId]_[timestamp]`
   - **列族**：`cf1`
   - **列**：根据设备类型动态生成

### HBase测试程序功能

测试程序提供以下功能菜单：

1. **插入示例传感器数据** - 插入预定义的测试数据
2. **查询指定设备数据** - 根据设备ID和时间戳查询
3. **扫描设备历史数据** - 查看设备的历史数据记录
4. **测试传感器数据生成** - 启动实时数据生成
5. **批量插入测试数据** - 批量插入多种类型的测试数据

### 设备类型和数据格式

根据您的数据库设置，支持以下设备类型：

| 设备类型 | en_name | 生成的数据字段 |
|---------|---------|---------------|
| 温度传感器 | temperature | temperature, unit |
| 湿度传感器 | humidity | humidity, unit |
| 空气成分传感器 | air_component | co2, o2, pm25, 各自的unit |
| 土壤NPK传感器 | soil_NPK | nitrogen, phosphorus, potassium, unit |
| PH值传感器 | soil_PH | ph, unit |
| 微量元素传感器 | soil_trace_elements | iron, zinc, copper, manganese, unit |
| 风速传感器 | wind_speed | wind_speed, unit |
| 风向传感器 | wind_direction | wind_direction, unit |
| 光照强度传感器 | light_intensity | illuminance, unit |

### 示例操作

#### 触发设备数据生成
```bash
# 开启设备13（温度传感器）
redis-cli RPUSH temperature '{"deviceId":"13","state":1}'

# 开启设备14（湿度传感器）  
redis-cli RPUSH humidity '{"deviceId":"14","state":1}'

# 开启设备10（土壤NPK传感器）
redis-cli RPUSH soil_NPK '{"deviceId":"10","state":1}'
```

#### 查看Kafka消息
```bash
# 消费Kafka主题查看生成的数据
kafka-console-consumer.sh --bootstrap-server 192.168.56.11:9092 --topic sensor_data_topic --from-beginning
```

#### 查看HBase数据
```bash
# 在HBase shell中查看数据
hbase shell
scan 'iot_sensor_data', {LIMIT => 10}
get 'iot_sensor_data', '13_1735689600000'
```

### 日志输出

消费到的数据会以以下格式打印：

```
[2024-01-15 10:30:00] Topic: temperature | Data: {"deviceId":"13","state":1}
Generated and stored sensor data for device 13 (temperature): {temperature=25.3, unit=°C, battery_level=87, signal_strength=-45, template_info={"type":"temperature","range":"-40~80","unit":"celsius","precision":"0.1"}}
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