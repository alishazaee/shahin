package org.shahin.configs;

public class ApplicationConfig {
    private KafkaConfig kafkaConfig;
    private ParquetWriterConf parquetWriter;
    private int queueCapacity;

    public ApplicationConfig() {
    }

    public ApplicationConfig(KafkaConfig kafkaConfig, ParquetWriterConf ingester, int queueCapacity ) {
        this.kafkaConfig = kafkaConfig;
        this.parquetWriter = ingester;
        this.queueCapacity = queueCapacity;
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

    public void setParquetWriterConf(ParquetWriterConf parquetWriter) {
        this.parquetWriter = parquetWriter;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }
}
