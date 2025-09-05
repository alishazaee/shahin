package org.shahin.eventhadnler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaUtils {
    private KafkaUtils() {
    }
    public static Consumer<Long, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("max.poll.interval.ms", "150000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 45000);
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 100);

        props.put("enable.auto.commit", "false");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        return new KafkaConsumer<>(props);
    }

}
