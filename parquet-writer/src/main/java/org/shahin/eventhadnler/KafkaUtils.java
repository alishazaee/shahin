package org.shahin.eventhadnler;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Properties;

public class KafkaUtils {
    private KafkaUtils() {
    }
    public static Consumer<Long, String> createConsumer(String bootstrapServers, String groupId) {
        Properties props = new Properties();

        props.put("bootstrap.servers", bootstrapServers);
        props.put("group.id", groupId);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        return new KafkaConsumer<>(props);
    }

}
