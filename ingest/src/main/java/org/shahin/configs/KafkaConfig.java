package org.shahin.configs;

import org.apache.commons.lang3.Validate;

import java.util.Properties;

public class KafkaConfig {

    private String bootstrapServers;
    private String compressionType;

    public KafkaConfig() {
    }

    public KafkaConfig(String bootstrapServers, String compressionType) {
        this.bootstrapServers = bootstrapServers;
        this.compressionType = compressionType;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getCompressionType() {
        return compressionType;
    }

    public void setCompressionType(String compressionType) {
        this.compressionType = compressionType;
    }

    public Properties getProperties() {
        Validate.isTrue(bootstrapServers != null && !bootstrapServers.isEmpty());
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put("batch.size", 32768);

        if (compressionType != null && !compressionType.isEmpty()) {
            properties.put("compression.type", compressionType);
        }

        return properties;
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig {"
                + "bootstrapServers='" + bootstrapServers + '\''
                + ", compressionType='" + compressionType + '\''
                + '}';
    }
}