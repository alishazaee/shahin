package org.shahin.utils;

import org.shahin.protobuf.NetRecordProto;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class Parser {
    private Parser() {}

    public static NetRecordProto.NetRecord DecodeToProto(String encodedRecord) {
        String[] byteStrings = encodedRecord.replace("[", "").replace("]", "").split(",");
        byte[] byteArray = new byte[byteStrings.length];
        for (int i = 0; i < byteStrings.length; i++) {
            byteArray[i] = Byte.parseByte(byteStrings[i].trim());
        }
        try {
            return NetRecordProto.NetRecord.parseFrom(byteArray);
        } catch (Exception e) {
            LoggerFactory.getLogger(Parser.class).error(e.getMessage());
            return null;
        }
    }


}
