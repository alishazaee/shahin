package org.shahin.service;

import org.shahin.configs.ApplicationConfig;
import org.shahin.protobuf.NetRecordProto;

import java.util.concurrent.BlockingQueue;

public class HandlerFactory {

    public NetFlowHandler createHandler(StorageLayer storageLayer, BlockingQueue<NetRecordProto.NetRecord> queue, ApplicationConfig config) {
        if (storageLayer == null) {
            throw new IllegalArgumentException("Storage handler cannot be null");
        } else if (storageLayer == StorageLayer.HDFS) {
            return new NetFlowHDFS(queue,config);
        }

        throw new IllegalArgumentException("Storage handler is invalid");
    }

}
