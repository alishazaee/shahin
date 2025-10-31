package org.shahin.eventhandler;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.shahin.configs.KafkaConfig;

import java.util.Properties;

public class KafkaUtils  {

    private KafkaUtils() {
    }

    public static KafkaProducer<byte[], byte[]> createProducer(String clientId, KafkaConfig kafkaProducerConfig) {
        Properties producerConfigs = new Properties();
        producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerConfig.getBootstrapServers());
        producerConfigs.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, kafkaProducerConfig.getCompressionType());
        producerConfigs.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        producerConfigs.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Integer.MAX_VALUE);
        producerConfigs.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.MAX_VALUE);
        producerConfigs.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfigs.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        producerConfigs.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
        return new KafkaProducer<>(producerConfigs);
    }

}
