package org.shahin.configs;

public class ApplicationConfig {
    KafkaConfig kafkaConfig;
    ParquetWriterConfig parquetWriter;
    int prometheusPort;

    public ApplicationConfig() {
    }

    public ApplicationConfig(KafkaConfig kafkaConfig, ParquetWriterConfig parquetWriter) {
        this.kafkaConfig = kafkaConfig;
        this.parquetWriter = parquetWriter;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }

    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }

    public ParquetWriterConfig getParquetWriterConf() {
        return parquetWriter;
    }

    public void setParquetWriterConf(ParquetWriterConfig parquetWriter) {
        this.parquetWriter = parquetWriter;
    }

    public int getPrometheusPort() {
        return prometheusPort;
    }

    public void setPrometheusPort(int prometheusPort) {
        this.prometheusPort = prometheusPort;
    }
}
