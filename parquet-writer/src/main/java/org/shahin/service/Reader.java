package org.shahin.service;

import org.apache.commons.collections.map.HashedMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.shahin.configs.ApplicationConfig;
import org.shahin.eventhadnler.KafkaUtils;
import org.shahin.protobuf.NetRecordProto;
import org.shahin.utils.Parser;

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader{
    private final BlockingQueue<KafkaRecord<NetRecordProto.NetRecord>> KafkaRecords;
    private final ApplicationConfig ApplicationConfig;
    private final Logger logger;
    private final BlockingQueue<AckRecord> AckQueue;

    public Reader(ApplicationConfig Conf, BlockingQueue<KafkaRecord<NetRecordProto.NetRecord>> records, BlockingQueue<AckRecord> ackQueue) {
        this.ApplicationConfig = Conf;
        this.KafkaRecords = records;
        this.AckQueue = ackQueue;
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public void Start(){

        Thread consumer = new Thread(() -> {
            Consumer<Long, String> Consumer = KafkaUtils.createConsumer(
                    ApplicationConfig.getKafkaConfig().getBootstrapServers(),
                    ApplicationConfig.getKafkaConfig().getgroupId());

            Consumer.subscribe(Collections.singletonList(ApplicationConfig.getParquetWriterConf().getNetworkLogsTopic()));
            logger.info("Starting Consumer with thread {}", Thread.currentThread().getName());

            while (true) {
                //ack phase

                Map<TopicPartition,Long> latestOffsets =  new HashMap<>();
                AckRecord ack;
                while ((ack = AckQueue.poll()) != null) {
                    TopicPartition tp = new TopicPartition(ack.getTopic(), ack.getPartition());
                    latestOffsets.put(tp, ack.getOffset());
                }

                if (!latestOffsets.isEmpty()) {
                    Map<TopicPartition, OffsetAndMetadata> commitMap = new HashMap<>();
                    latestOffsets.forEach((tp, offset) -> {
                        commitMap.put(tp, new OffsetAndMetadata(offset + 1));
                    });
                    Consumer.commitAsync(
                            commitMap,
                            (offset, ex) -> {
                                if (ex != null) {
                                    logger.error(ex.getMessage());
                                }
                            });
                }

                //consume phase
                ConsumerRecords<Long, String> records = Consumer.poll(Duration.ofMillis(10));
                for (ConsumerRecord<Long, String> record : records) {
                    NetRecordProto.NetRecord netRecord = Parser.DecodeToProto(record.value());

                    try {
                        KafkaRecords.put(new KafkaRecord<>(record.topic(), record.partition(), record.offset(), netRecord));
                    } catch (InterruptedException e) {
                        this.logger.info(e.getMessage());
                    }
                }
            }
        });
            consumer.start();

    }

}