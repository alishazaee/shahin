package org.shahin.configs;

import org.apache.commons.lang3.Validate;

import java.util.Properties;

public class KafkaConsumerConfig {


    String bootstrapServers;
    String groupId;

    public KafkaConsumerConfig() {
    }

    public KafkaConsumerConfig(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;

    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getGroupId() {
        return groupId;
    }


    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public Properties getProperties() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("group.id", groupId);
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("enable.auto.commit", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.put("max.poll.interval.ms", "30000");
        return properties;
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig {"
                + "bootstrapServers='" + bootstrapServers + '\''
                + ", groupId='" + groupId + '\''
                + '}';
    }
}