package org.shahin.utils;
import org.shahin.protobuf.NetRecordProto;

public class Parser {
    private Parser() {

    }

    public static byte[] EncodeNetRecord(String[] netRecord, String recordedTime){
        NetRecordProto.NetRecord record =  NetRecordProto.NetRecord.newBuilder()
                .setIpv4SrcAddr(netRecord[0])
                .setL4SrcPort(Integer.parseInt(netRecord[1]))
                .setIpv4DstAddr(netRecord[2])
                .setL4DstPort(Integer.parseInt(netRecord[3]))
                .setInBytes(Integer.parseInt(netRecord[6]))
                .setOutBytes(Integer.parseInt(netRecord[8]))
                .setDate(recordedTime.split("_")[0])
                .setHour(Integer.parseInt(recordedTime.split("_")[1]))
                .setMinute(Integer.parseInt(recordedTime.split("_")[2]))
                .build();
        return record.toByteArray();
    }

}
