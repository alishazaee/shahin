package org.shahin.utils;
import org.shahin.Parser;
import org.shahin.protobuf.NetRecordProto;

public class NetRecordParser implements Parser {

    public  byte[] Encode(String line){
        long epochSeconds = System.currentTimeMillis() / 1000;

        String[] netRecord = line.split(",");
        NetRecordProto.NetRecord record =  NetRecordProto.NetRecord.newBuilder()
                .setIpv4SrcAddr(netRecord[0])
                .setL4SrcPort(Integer.parseInt(netRecord[1]))
                .setIpv4DstAddr(netRecord[2])
                .setL4DstPort(Integer.parseInt(netRecord[3]))
                .setInBytes(Integer.parseInt(netRecord[6]))
                .setOutBytes(Integer.parseInt(netRecord[8]))
                .setDate(String.valueOf(epochSeconds))
                .build();
        return record.toByteArray();
    }

}
