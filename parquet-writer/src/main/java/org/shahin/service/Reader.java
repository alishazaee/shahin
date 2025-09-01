package org.shahin.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.shahin.configs.ApplicationConfig;
import org.shahin.eventhadnler.KafkaUtils;
import org.shahin.protobuf.NetRecordProto;
import org.shahin.utils.Parser;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.BlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Reader implements Runnable{
    private final BlockingQueue<NetRecordProto.NetRecord> Queue;
    private final ApplicationConfig ApplicationConfig;
    private final  Consumer<Long,String> Consumer ;
    private final Logger Logger;

    public Reader(ApplicationConfig Conf, BlockingQueue<NetRecordProto.NetRecord> queue) {
        this.ApplicationConfig = Conf;
        this.Queue = queue;
        this.Consumer = KafkaUtils.createConsumer(Conf.getKafkaConfig().getBootstrapServers(),Conf.getKafkaConfig().getgroupId());
        Logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public void run(){
        Consumer.subscribe(Collections.singletonList(ApplicationConfig.getParquetWriterConf().getNetworkLogsTopic()));
        while(true){
            ConsumerRecords<Long, String> records = Consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<Long, String> record : records) {
                NetRecordProto.NetRecord netRecord = Parser.DecodeToProto(record.value());
                try {
                    Queue.put(netRecord);
                } catch (InterruptedException e) {
                    this.Logger.info(e.getMessage());
                }
            }
        }
    }

}