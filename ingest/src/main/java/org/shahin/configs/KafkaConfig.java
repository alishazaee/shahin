package org.shahin.configs;


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

    @Override
    public String toString() {
        return "KafkaProducerConfig {"
                + "bootstrapServers='" + bootstrapServers + '\''
                + ", compressionType='" + compressionType + '\''
                + '}';
    }
}