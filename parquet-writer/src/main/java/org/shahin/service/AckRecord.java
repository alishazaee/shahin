package org.shahin.service;

public class AckRecord {
    private final String topic;
    private  final int partition;
    private final long offset;

    public AckRecord(String topic, int partition, long offset) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }
}
