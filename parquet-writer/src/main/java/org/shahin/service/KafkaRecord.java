package org.shahin.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import java.util.Map;

public class KafkaRecord <T> {
    private final String topic;
    private final int partition;
    private final long offset;
    private final T message;

    public KafkaRecord(String topic, int partition, long offset, T message ) {
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.message = message;
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

    public T getMessage() {
        return message;
    }




}
