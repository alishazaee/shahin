package org.shahin.configs;

public class ApplicationConfig {
    private KafkaConfig kafkaConfig;
    private ParquetWriterConf parquetWriter;

    public ApplicationConfig() {
    }

    public ApplicationConfig(KafkaConfig kafkaConfig, ParquetWriterConf ingester ) {
        this.kafkaConfig = kafkaConfig;
        this.parquetWriter = ingester;
    }

    public KafkaConfig getKafkaConfig() {
        return kafkaConfig;
    }
    public void setKafkaConfig(KafkaConfig kafkaConfig) {
        this.kafkaConfig = kafkaConfig;
    }
    public ParquetWriterConf getParquetWriterConf() {
        return parquetWriter;
    }
    public void setParquetWriterConf(ParquetWriterConf conf) {
        this.parquetWriter = conf;
    }
}
