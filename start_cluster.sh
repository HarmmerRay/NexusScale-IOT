#!/bin/bash

# 集群服务启动脚本
# 在master机器(192.168.56.10)上执行
# 作者: NexusScale-IOT
# 描述: 启动4台虚拟机组成的大数据集群的所有服务

# 设置颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 集群节点IP
MASTER="192.168.56.10"
SLAVE1="192.168.56.11" 
SLAVE2="192.168.56.12"
SLAVE3="192.168.56.13"
SLAVES=("$SLAVE1" "$SLAVE2" "$SLAVE3")

# 组件路径配置
BASE_DIR="/home/zy/下载"
KAFKA_HOME="$BASE_DIR/kafka_2.13-3.0.0"
NODE_EXPORTER_HOME="$BASE_DIR/node_exporter-1.9.0.linux-amd64"
PROMETHEUS_HOME="$BASE_DIR/prometheus-3.2.1.linux-amd64"
GRAFANA_HOME="$BASE_DIR/grafana-v11.3.3"
HADOOP_HOME="$BASE_DIR/hadoop-3.1.3"
HBASE_HOME="$BASE_DIR/hbase-2.2.2"
ZOOKEEPER_HOME="$BASE_DIR/zookeeper-3.4.14"

# 日志函数
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_step() {
    echo -e "${BLUE}[STEP]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 检查SSH连接
check_ssh_connectivity() {
    log_step "检查SSH连接..."
    
    for slave in "${SLAVES[@]}"; do
        if ssh -o ConnectTimeout=5 $slave "echo 'SSH OK'" >/dev/null 2>&1; then
            log_info "SSH连接到 $slave 成功"
        else
            log_error "无法SSH连接到 $slave"
            exit 1
        fi
    done
}

# 清理本地数据（不需要服务运行）
cleanup_local_data() {
    log_step "清理本地数据..."
    
    # 清理所有节点的本地HBase和Kafka数据
    log_info "清理本地HBase和Kafka数据..."
    
    # 清理master节点
    rm -rf /tmp/hbase-* 2>/dev/null || true
    rm -rf $HBASE_HOME/logs/* 2>/dev/null || true
    
    # 清理slave节点
    for slave in "${SLAVES[@]}"; do
        log_info "清理 $slave 节点数据..."
        ssh $slave "
            BASE_DIR='/home/zy/下载'
            HBASE_HOME='\$BASE_DIR/hbase-2.2.2'
            rm -rf /tmp/hbase-* 2>/dev/null || true
            rm -rf \$HBASE_HOME/logs/* 2>/dev/null || true
            rm -rf /tmp/kafka-logs/* 2>/dev/null || true
            find /tmp -name 'meta.properties' -delete 2>/dev/null || true
        " &
    done
    wait
    
    log_info "本地数据清理完成"
}

# 清理HDFS和ZooKeeper中的旧数据（需要服务运行）
cleanup_service_data() {
    log_step "清理HDFS和ZooKeeper中的旧数据..."
    
    # 清理HDFS中的HBase目录
    log_info "清理HDFS中的HBase目录..."
    source /etc/profile
    hdfs dfs -rm -r -f /hbase 2>/dev/null || true
    
    # 清理ZooKeeper中的HBase和Kafka节点
    log_info "清理ZooKeeper数据..."
    ssh $SLAVE1 "source /etc/profile && zkCli.sh <<EOF
ls /
rmr /hbase 2>/dev/null || true
rmr /brokers 2>/dev/null || true
rmr /admin 2>/dev/null || true
rmr /isr_change_notification 2>/dev/null || true
rmr /consumers 2>/dev/null || true
rmr /config 2>/dev/null || true
rmr /controller_epoch 2>/dev/null || true
rmr /cluster 2>/dev/null || true
rmr /feature 2>/dev/null || true
rmr /latest_producer_id_block 2>/dev/null || true
rmr /log_dir_event_notification 2>/dev/null || true
quit
EOF" 2>/dev/null || true
    
    log_info "服务数据清理完成"
}

# 启动Hadoop
start_hadoop() {
    log_step "启动Hadoop..."
    
    # 加载环境变量
    source /etc/profile
    
    log_info "启动HDFS..."
    start-dfs.sh
    
    log_info "启动YARN..."
    start-yarn.sh
    
    # 等待服务启动
    sleep 10
    
    # 验证Hadoop服务
    if jps | grep -q "NameNode"; then
        log_info "HDFS NameNode 启动成功"
    else
        log_error "HDFS NameNode 启动失败"
        return 1
    fi
    
    if jps | grep -q "ResourceManager"; then
        log_info "YARN ResourceManager 启动成功"
    else
        log_error "YARN ResourceManager 启动失败"
        return 1
    fi
    
    log_info "Hadoop启动完成"
    log_info "Hadoop Web UI: http://$MASTER:9870 (HDFS)"
    log_info "YARN Web UI: http://$MASTER:8088 (YARN)"
}

# 启动ZooKeeper
start_zookeeper() {
    log_step "启动ZooKeeper..."
    
    for slave in "${SLAVES[@]}"; do
        log_info "在 $slave 上启动ZooKeeper..."
        ssh $slave "source /etc/profile && zkServer.sh start" &
    done
    wait
    
    # 等待ZooKeeper启动
    sleep 15
    
    # 验证ZooKeeper状态
    for slave in "${SLAVES[@]}"; do
        log_info "检查 $slave 上的ZooKeeper状态..."
        if ssh $slave "source /etc/profile && zkServer.sh status" | grep -q "Mode:"; then
            log_info "$slave ZooKeeper 启动成功"
        else
            log_error "$slave ZooKeeper 启动失败"
            return 1
        fi
    done
    
    log_info "ZooKeeper集群启动完成"
}

# 启动HBase
start_hbase() {
    log_step "启动HBase..."
    
    # 加载环境变量
    source /etc/profile
    
    log_info "启动HBase集群..."
    start-hbase.sh
    
    # 等待HBase启动
    sleep 20
    
    # 验证HBase服务
    if jps | grep -q "HMaster"; then
        log_info "HBase Master 启动成功"
    else
        log_error "HBase Master 启动失败"
        return 1
    fi
    
    # 创建IoT传感器数据表
    log_info "创建HBase表..."
    source /etc/profile
    hbase shell <<EOF
create 'iot_sensor_data','cf1'
list
exit
EOF
    
    log_info "HBase启动完成"
    log_info "HBase Web UI: http://$MASTER:16010"
}

# 启动Kafka
start_kafka() {
    log_step "启动Kafka..."
    
    for slave in "${SLAVES[@]}"; do
        log_info "在 $slave 上启动Kafka..."
        ssh $slave "
            BASE_DIR='/home/zy/下载'
            KAFKA_HOME='\$BASE_DIR/kafka_2.13-3.0.0'
            cd \$KAFKA_HOME && ./bin/kafka-server-start.sh -daemon config/server.properties
        " &
    done
    wait
    
    # 等待Kafka启动
    sleep 15
    
    # 验证Kafka服务
    for slave in "${SLAVES[@]}"; do
        if ssh $slave "jps | grep -q Kafka"; then
            log_info "$slave Kafka 启动成功"
        else
            log_error "$slave Kafka 启动失败"
            return 1
        fi
    done
    
    # 创建IoT传感器数据主题
    log_info "创建Kafka主题..."
    source /etc/profile
    $KAFKA_HOME/bin/kafka-topics.sh --create --bootstrap-server $SLAVE1:9092,$SLAVE2:9092,$SLAVE3:9092 \
        --topic sensor_data_topic --partitions 3 --replication-factor 2 2>/dev/null || true
    
    # 验证主题创建
    log_info "验证Kafka主题..."
    $KAFKA_HOME/bin/kafka-topics.sh --list --bootstrap-server $SLAVE1:9092,$SLAVE2:9092,$SLAVE3:9092
    
    log_info "Kafka集群启动完成"
}

# 启动监控服务
start_monitoring() {
    log_step "启动监控服务..."
    
    # 启动所有节点的Node Exporter
    log_info "启动Node Exporter..."
    for node in $MASTER "${SLAVES[@]}"; do
        log_info "在 $node 上启动Node Exporter..."
        if [ "$node" == "$MASTER" ]; then
            nohup $NODE_EXPORTER_HOME/node_exporter > /dev/null 2>&1 &
        else
            ssh $node "
                BASE_DIR='/home/zy/下载'
                NODE_EXPORTER_HOME='\$BASE_DIR/node_exporter-1.9.0.linux-amd64'
                nohup \$NODE_EXPORTER_HOME/node_exporter > /dev/null 2>&1 &
            " &
        fi
    done
    wait
    
    # 启动Prometheus (仅在master)
    log_info "启动Prometheus..."
    cd $PROMETHEUS_HOME
    nohup ./prometheus --config.file=prometheus.yml > /dev/null 2>&1 &
    
    # 启动Grafana (仅在master)
    log_info "启动Grafana..."
    cd $GRAFANA_HOME/bin
    nohup ./grafana-server web > /dev/null 2>&1 &
    
    sleep 5
    
    log_info "监控服务启动完成"
    log_info "Prometheus Web UI: http://$MASTER:9090"
    log_info "Grafana Web UI: http://$MASTER:3000 (admin/111111)"
}

# 显示集群状态
show_cluster_status() {
    log_step "集群状态总览"
    echo
    echo "================================="
    echo "    集群服务启动完成!"
    echo "================================="
    echo
    echo "🌐 Web界面访问地址:"
    echo "  📊 Hadoop HDFS:    http://$MASTER:9870"
    echo "  🔧 YARN:           http://$MASTER:8088"  
    echo "  🗃️  HBase:          http://$MASTER:16010"
    echo "  📈 Prometheus:     http://$MASTER:9090"
    echo "  📉 Grafana:        http://$MASTER:3000 (admin/111111)"
    echo
    echo "🔗 服务连接信息:"
    echo "  📊 Kafka Brokers:  $SLAVE1:9092,$SLAVE2:9092,$SLAVE3:9092"
    echo "  🔍 ZooKeeper:      $SLAVE1:2181,$SLAVE2:2181,$SLAVE3:2181"
    echo "  🗃️  HBase:          $MASTER:16000"
    echo
    echo "🚀 所有服务已成功启动!"
    echo "================================="
}

# 主函数
main() {
    echo "=================================================="
    echo "       NexusScale-IOT 集群启动脚本"
    echo "=================================================="
    echo
    
    # 检查是否在master节点执行
    current_ip=$(hostname -I | awk '{print $1}')
    if [ "$current_ip" != "$MASTER" ]; then
        log_warn "建议在master节点($MASTER)上执行此脚本"
    fi
    
    # 执行启动流程
    check_ssh_connectivity
    cleanup_local_data           # 先清理本地数据
    start_hadoop                 # 启动Hadoop
    start_zookeeper             # 启动ZooKeeper
    cleanup_service_data        # 清理HDFS和ZooKeeper中的旧数据
    start_hbase                 # 启动HBase
    start_kafka                 # 启动Kafka
    start_monitoring            # 启动监控服务
    show_cluster_status
    
    log_info "集群启动脚本执行完成!"
}

# 错误处理
set -e
trap 'log_error "脚本执行失败，请检查错误信息"; exit 1' ERR

# 执行主函数
main "$@" 