package org.shahin.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.shahin.configs.ApplicationConfig;
import org.shahin.protobuf.NetRecordProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class Writer implements Runnable {
    private final BlockingQueue<KafkaRecord<NetRecordProto.NetRecord>> RecordQueue;
    private final BlockingQueue<AckRecord> AckQueue;
    private final ApplicationConfig ApplicationConfig;
    private final Logger logger ;

    public Writer(ApplicationConfig Conf, BlockingQueue<KafkaRecord<NetRecordProto.NetRecord>> RecordQueue, BlockingQueue<AckRecord> AckQueue) {
        this.ApplicationConfig = Conf;
        this.RecordQueue = RecordQueue;
        this.logger = LoggerFactory.getLogger(this.getClass());
        this.AckQueue = AckQueue;
    }

    @Override
    public void run() {
        logger.info("Starting writer with thread {}" , Thread.currentThread().getName());
        HandlerFactory factory = new HandlerFactory();
        NetFlowHandler handler = factory.createHandler(StorageLayer.HDFS,
                RecordQueue,
                ApplicationConfig,
                AckQueue);
        handler.handle();

    }


}
