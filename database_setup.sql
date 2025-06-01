-- NexusScale IoT 数据库初始化脚本
-- 创建数据库
CREATE DATABASE IF NOT EXISTS nexuscale_iot CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE nexuscale_iot;

-- 创建设备模板表
CREATE TABLE IF NOT EXISTS device_template (
    id INT AUTO_INCREMENT PRIMARY KEY,
    en_name VARCHAR(255) NOT NULL COMMENT '设备类型英文名称',
    cn_name VARCHAR(255) COMMENT '设备类型中文名称',
    template TEXT COMMENT '设备模板配置JSON',
    description TEXT COMMENT '设备描述',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_en_name (en_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备模板表';

-- 创建设备表
CREATE TABLE IF NOT EXISTS device (
    id INT AUTO_INCREMENT PRIMARY KEY,
    device_name VARCHAR(255) NOT NULL COMMENT '设备名称',
    device_code VARCHAR(100) UNIQUE COMMENT '设备编码',
    template_id INT NOT NULL COMMENT '设备模板ID',
    location VARCHAR(255) COMMENT '设备位置',
    status TINYINT DEFAULT 0 COMMENT '设备状态：0-离线，1-在线',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (template_id) REFERENCES device_template(id),
    INDEX idx_template_id (template_id),
    INDEX idx_status (status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备表';

-- 插入设备模板数据
INSERT INTO device_template (en_name, cn_name, template, description) VALUES
('temperature_sensor', '温度传感器', '{"type":"temperature","range":"-40~80","unit":"celsius","precision":"0.1"}', '用于测量环境温度的传感器'),
('humidity_sensor', '湿度传感器', '{"type":"humidity","range":"0~100","unit":"percent","precision":"0.1"}', '用于测量环境湿度的传感器'),
('motion_detector', '运动检测器', '{"type":"motion","range":"0~10","unit":"meters","detection_angle":"120"}', '用于检测运动的PIR传感器'),
('smart_camera', '智能摄像头', '{"type":"camera","resolution":"1920x1080","fps":"30","storage":"local"}', '具有智能分析功能的摄像头'),
('pressure_sensor', '压力传感器', '{"type":"pressure","range":"300~1100","unit":"hPa","precision":"0.01"}', '用于测量大气压力的传感器'),
('light_sensor', '光照传感器', '{"type":"light","range":"0~100000","unit":"lux","precision":"1"}', '用于测量环境光照强度的传感器');

-- 插入测试设备数据
INSERT INTO device (device_name, device_code, template_id, location, status) VALUES
('会议室温度传感器', 'TEMP_001', 1, '会议室A', 1),
('办公区湿度传感器', 'HUMI_001', 2, '办公区1', 1),
('大厅运动检测器', 'MOTION_001', 3, '大厅入口', 1),
('停车场摄像头', 'CAMERA_001', 4, '停车场', 1),
('气象站压力传感器', 'PRESS_001', 5, '屋顶气象站', 1),
('室外光照传感器', 'LIGHT_001', 6, '室外平台', 1);

-- 创建设备状态历史表（可选，用于记录设备状态变化）
CREATE TABLE IF NOT EXISTS device_status_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    device_id INT NOT NULL,
    old_status TINYINT,
    new_status TINYINT,
    change_reason VARCHAR(255),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (device_id) REFERENCES device(id),
    INDEX idx_device_id (device_id),
    INDEX idx_changed_at (changed_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='设备状态历史表';

-- 查询验证数据
SELECT 
    d.id as device_id,
    d.device_name,
    d.device_code,
    dt.en_name as device_type,
    dt.cn_name as device_type_cn,
    dt.template,
    d.location,
    d.status
FROM device d
JOIN device_template dt ON d.template_id = dt.id
ORDER BY d.id;

-- 查询所有设备类型
SELECT DISTINCT en_name FROM device_template WHERE en_name IS NOT NULL AND en_name != '';

-- 显示表结构
DESCRIBE device_template;
DESCRIBE device; 