package org.shahin.service;

import org.shahin.configs.ApplicationConfig;
import org.shahin.protobuf.NetRecordProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;

public class Writer implements Runnable {
    private final BlockingQueue<NetRecordProto.NetRecord> Queue;
    private final ApplicationConfig ApplicationConfig;
    private final Logger logger ;

    public Writer(ApplicationConfig Conf, BlockingQueue<NetRecordProto.NetRecord> queue) {
        this.ApplicationConfig = Conf;
        this.Queue = queue;
        this.logger = LoggerFactory.getLogger(this.getClass());
    }

    @Override
    public void run() {
        HandlerFactory factory = new HandlerFactory();
        NetFlowHandler handler = factory.createHandler(StorageLayer.HDFS,Queue,ApplicationConfig);
        handler.handle();

    }


}
