package org.shahin.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.shahin.configs.ApplicationConfig;
import org.shahin.protobuf.NetRecordProto;

import java.util.concurrent.BlockingQueue;

public class HandlerFactory {

    public NetFlowHandler createHandler(StorageLayer storageLayer,
                                        BlockingQueue<KafkaRecord<NetRecordProto.NetRecord>> queue,
                                        ApplicationConfig config,
                                        BlockingQueue<AckRecord> AckQueue) {
        if (storageLayer == null) {
            throw new IllegalArgumentException("Storage handler cannot be null");
        } else if (storageLayer == StorageLayer.HDFS) {
            return new NetFlowHDFS(queue,config,AckQueue);
        }

        throw new IllegalArgumentException("Storage handler is invalid");
    }

}
