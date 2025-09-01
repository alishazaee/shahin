package org.shahin.service;

import org.apache.avro.generic.GenericRecord;
import org.shahin.protobuf.NetRecordProto;

public interface NetFlowHandler {
    void handle();
}
