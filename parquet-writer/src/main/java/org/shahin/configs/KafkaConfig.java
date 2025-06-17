package org.shahin.configs;


public class KafkaConfig {

    private String bootstrapServers;
    private String groupId;

    public KafkaConfig() {
    }

    public KafkaConfig(String bootstrapServers, String groupId) {
        this.bootstrapServers = bootstrapServers;
        this.groupId = groupId;

    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getgroupId() {
        return groupId;
    }

    public void setgroupId(String compressionType) {
        this.groupId = compressionType;
    }

    @Override
    public String toString() {
        return "KafkaProducerConfig {"
                + "bootstrapServers='" + bootstrapServers + '\''
                + ", groupId='" + groupId + '\''
                + '}';
    }
}