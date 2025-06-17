package org.shahin.repository.hdfs;

import org.apache.avro.protobuf.ProtobufData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.shahin.protobuf.NetRecordProto;
import org.shahin.utils.HDFSFile;


public class HDFS implements IHDFSParquetWriter{

    public ParquetWriter<NetRecordProto.NetRecord> writeToHdfs(String dirPath,String hdfsUrlPath){
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", hdfsUrlPath);

        Path outputPath = new Path(HDFSFile.getHDFSFileName(dirPath));
        try {
            return AvroParquetWriter.<NetRecordProto.NetRecord> builder(outputPath)
                    .withSchema(ProtobufData.get().getSchema(NetRecordProto.NetRecord.class))
                    .withDataModel(ProtobufData.get())
                    .withConf(conf)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Parquet writer", e);
        }
    }


}
