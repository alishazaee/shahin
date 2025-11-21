package org.shahin.configs;

public class ApplicationConfig {
    private KafkaConsumerConfig kafkaConfig;
    private Enricher enricher;

    public ApplicationConfig() {
    }

    public ApplicationConfig(KafkaConsumerConfig kafkaConfig, Enricher enricher ) {
        this.kafkaConfig = kafkaConfig;
        this.enricher = enricher;
    }

    public KafkaConsumerConfig getKafkaConfig() {
        return kafkaConfig;
    }
    public void setKafkaConfig(KafkaConsumerConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }
    public Enricher getEnricher() {
        return enricher;
    }
    public void setEnricher(Enricher enricher) {
        this.enricher = enricher;
    }
}
