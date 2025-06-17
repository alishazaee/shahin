package org.shahin.utils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class HDFSFile {
    private HDFSFile() {

    }
    public static String getHDFSFileName(String dirPath){
        LocalDateTime localDateTime = LocalDateTime.now();
        String date = DateTimeFormatter.ofPattern("yyyy-MM-dd").format(localDateTime);
        String time = DateTimeFormatter.ofPattern("HH").format(localDateTime);
        return dirPath + "/" + date  + "/" + time + "/" + UUID.randomUUID().toString().substring(0, 8) +".parquet";
    }
}
