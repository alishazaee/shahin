package org.shahin.configs;

public class ApplicationConfig {
    private KafkaConfig kafkaConfig;
    private Ingester ingester;

    public ApplicationConfig() {
    }

    public ApplicationConfig(KafkaConfig kafkaConfig, Ingester ingester ) {
        this.kafkaConfig = kafkaConfig;
        this.ingester = ingester;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }
    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }
    public Ingester getIngester() {
        return ingester;
    }
    public void setIngester(Ingester ingester) {
        this.ingester = ingester;
    }
}
