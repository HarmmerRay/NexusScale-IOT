package com.nexuscale.entity;

public abstract class SensorData {
    private final long timestamp;
    public SensorData(long timestamp) {
        this.timestamp = timestamp;
    }
    public long getTimestamp() {
        return timestamp;
    }

    public abstract String getDataDescription();
}
