package org.shahin.utils;
import org.shahin.Parser;
import org.shahin.protobuf.NetRecordProto;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Optional;

public class NetRecordParser implements Parser {

    public Optional<byte[]> encode(String line){
        if(line.startsWith("IPV4"))
            return Optional.empty();

        long epochSeconds = System.currentTimeMillis() / 1000;
        LocalDateTime timestamp = LocalDateTime.ofEpochSecond(epochSeconds, 0, ZoneOffset.UTC);

        String[] netRecord = line.split(",");
        NetRecordProto.NetRecord record =  NetRecordProto.NetRecord.newBuilder()
                .setIpv4SrcAddr(netRecord[0])
                .setL4SrcPort(Integer.parseInt(netRecord[1]))
                .setIpv4DstAddr(netRecord[2])
                .setL4DstPort(Integer.parseInt(netRecord[3]))
                .setInBytes(Integer.parseInt(netRecord[6]))
                .setOutBytes(Integer.parseInt(netRecord[8]))
                .setDate(String.valueOf(epochSeconds))
                .setHour(timestamp.getHour())
                .setMinute(timestamp.getMinute())
                .build();
        return Optional.ofNullable(record.toByteArray());
    }

}
