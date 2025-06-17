package org.shahin.repository.hdfs;

import org.apache.parquet.hadoop.ParquetWriter;
import org.shahin.protobuf.NetRecordProto;

public interface IHDFSParquetWriter {
    ParquetWriter<NetRecordProto.NetRecord> writeToHdfs(String dirPath,String hdfsUrlPath);
}
