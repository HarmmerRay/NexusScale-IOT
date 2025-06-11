#!/bin/bash

# 集群服务停止脚本
# 在master机器(192.168.56.10)上执行
# 作者: NexusScale-IOT
# 描述: 安全停止4台虚拟机组成的大数据集群的所有服务

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

# 停止监控服务
stop_monitoring() {
    log_step "停止监控服务..."
    
    # 停止Grafana
    log_info "停止Grafana..."
    pkill -f grafana-server 2>/dev/null || true
    
    # 停止Prometheus
    log_info "停止Prometheus..."
    pkill -f prometheus 2>/dev/null || true
    
    # 停止所有节点的Node Exporter
    log_info "停止Node Exporter..."
    for node in $MASTER "${SLAVES[@]}"; do
        if [ "$node" == "$MASTER" ]; then
            pkill -f node_exporter 2>/dev/null || true
        else
            ssh $node "pkill -f node_exporter 2>/dev/null || true" &
        fi
    done
    wait
    
    log_info "监控服务停止完成"
}

# 停止Kafka
stop_kafka() {
    log_step "停止Kafka..."
    
    for slave in "${SLAVES[@]}"; do
        log_info "在 $slave 上停止Kafka..."
        ssh $slave "
            BASE_DIR='/home/zy/下载'
            KAFKA_HOME='\$BASE_DIR/kafka_2.13-3.0.0'
            cd \$KAFKA_HOME && ./bin/kafka-server-stop.sh
        " &
    done
    wait
    
    # 等待Kafka完全停止
    sleep 10
    
    # 强制杀死残留进程
    for slave in "${SLAVES[@]}"; do
        ssh $slave "pkill -f kafka 2>/dev/null || true" &
    done
    wait
    
    log_info "Kafka集群停止完成"
}

# 停止HBase
stop_hbase() {
    log_step "停止HBase..."
    
    # 加载环境变量
    source /etc/profile
    
    log_info "停止HBase集群..."
    stop-hbase.sh
    
    # 等待HBase完全停止
    sleep 15
    
    # 强制杀死残留进程
    pkill -f HMaster 2>/dev/null || true
    pkill -f HRegionServer 2>/dev/null || true
    
    for slave in "${SLAVES[@]}"; do
        ssh $slave "pkill -f HRegionServer 2>/dev/null || true" &
    done
    wait
    
    log_info "HBase停止完成"
}

# 停止ZooKeeper
stop_zookeeper() {
    log_step "停止ZooKeeper..."
    
    for slave in "${SLAVES[@]}"; do
        log_info "在 $slave 上停止ZooKeeper..."
        ssh $slave "source /etc/profile && zkServer.sh stop" &
    done
    wait
    
    # 等待ZooKeeper完全停止
    sleep 10
    
    # 强制杀死残留进程
    for slave in "${SLAVES[@]}"; do
        ssh $slave "pkill -f QuorumPeerMain 2>/dev/null || true" &
    done
    wait
    
    log_info "ZooKeeper集群停止完成"
}

# 停止Hadoop
stop_hadoop() {
    log_step "停止Hadoop..."
    
    # 加载环境变量
    source /etc/profile
    
    log_info "停止YARN..."
    stop-yarn.sh
    
    log_info "停止HDFS..."
    stop-dfs.sh
    
    # 等待服务完全停止
    sleep 10
    
    # 强制杀死残留进程
    pkill -f NameNode 2>/dev/null || true
    pkill -f DataNode 2>/dev/null || true
    pkill -f ResourceManager 2>/dev/null || true
    pkill -f NodeManager 2>/dev/null || true
    
    for slave in "${SLAVES[@]}"; do
        ssh $slave "
            pkill -f DataNode 2>/dev/null || true
            pkill -f NodeManager 2>/dev/null || true
        " &
    done
    wait
    
    log_info "Hadoop停止完成"
}

# 检查进程状态
check_processes() {
    log_step "检查残留进程..."
    
    log_info "Master节点 ($MASTER) 进程状态:"
    jps | grep -E "(NameNode|ResourceManager|HMaster|Kafka|QuorumPeerMain)" || echo "  无相关进程运行"
    
    for slave in "${SLAVES[@]}"; do
        log_info "$slave 节点进程状态:"
        ssh $slave "jps | grep -E '(DataNode|NodeManager|HRegionServer|Kafka|QuorumPeerMain)'" || echo "  无相关进程运行"
    done
}

# 显示停止状态
show_stop_status() {
    log_step "集群停止状态总览"
    echo
    echo "================================="
    echo "    集群服务停止完成!"
    echo "================================="
    echo
    echo "🛑 已停止的服务:"
    echo "  📊 Hadoop (HDFS + YARN)"
    echo "  🔍 ZooKeeper集群"  
    echo "  🗃️  HBase集群"
    echo "  📊 Kafka集群"
    echo "  📈 Prometheus + Grafana"
    echo "  📊 Node Exporter"
    echo
    echo "✅ 所有服务已安全停止!"
    echo "================================="
}

# 主函数
main() {
    echo "=================================================="
    echo "       NexusScale-IOT 集群停止脚本"
    echo "=================================================="
    echo
    
    # 检查是否在master节点执行
    current_ip=$(hostname -I | awk '{print $1}')
    if [ "$current_ip" != "$MASTER" ]; then
        log_warn "建议在master节点($MASTER)上执行此脚本"
    fi
    
    # 按相反顺序停止服务
    stop_monitoring
    stop_kafka
    stop_hbase
    stop_zookeeper
    stop_hadoop
    
    # 检查进程状态
    sleep 5
    check_processes
    
    show_stop_status
    
    log_info "集群停止脚本执行完成!"
}

# 执行主函数
main "$@" 