package org.shahin.service;
import org.apache.avro.protobuf.ProtobufData;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.shahin.configs.ApplicationConfig;
import org.shahin.protobuf.NetRecordProto;
import org.apache.hadoop.fs.Path;
import org.shahin.utils.Parser;

import java.io.IOException;
import java.util.Calendar;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

public class NetFlowHDFS implements NetFlowHandler, AutoCloseable {
    private final String BasePath;
    private final Configuration Conf;
    private final ApplicationConfig AppConfig;
    private final BlockingQueue<NetRecordProto.NetRecord> Queue;
    private  WriteHolder holder ;


    public NetFlowHDFS(BlockingQueue<NetRecordProto.NetRecord> queue, ApplicationConfig appConfig) {
        Conf = new Configuration();
        Conf.set("fs.defaultFS", appConfig.getParquetWriterConf().getHdfsUrl());
        BasePath = appConfig.getParquetWriterConf().getHdfsDirPath();
        Queue = queue;
        AppConfig = appConfig;
    }


    private Path getHDFSPath(){
        String Year = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
        String month = String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1);
        String day = String.valueOf(Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
        return new Path(BasePath + "/" + Year + "/" + month + "/"  + day + "/"+  UUID.randomUUID() + ".parquet");
    }



    private class WriteHolder{
        private final ParquetWriter<NetRecordProto.NetRecord> writer;
        private  int count;
        private WriteHolder(ParquetWriter<NetRecordProto.NetRecord> writer) {
            this.writer = writer;
            count = 0;
        }
    }

    private WriteHolder createHolder(){
        ParquetWriter<NetRecordProto.NetRecord> writer;
        try {
             writer = AvroParquetWriter.<NetRecordProto.NetRecord> builder( getHDFSPath())
                    .withSchema(ProtobufData.get().getSchema(NetRecordProto.NetRecord.class))
                    .withDataModel(ProtobufData.get())
                    .withConf(Conf)
                    .withRowGroupSize(128 * 1024 * 1024)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .build();
        } catch (Exception e) {
            throw new RuntimeException("Failed to create Parquet writer", e);
        }
        return new WriteHolder(writer);
    }

    @Override
    public void close() throws Exception {
        if (holder != null && holder.writer != null) {
            holder.writer.close();
        }
    }

    @Override
    public void handle() {
        holder = createHolder();
        for (;;) {
            try{
                NetRecordProto.NetRecord record = Queue.take();
                if(holder.writer.getDataSize() / (1024.0 * 1024.0) < AppConfig.getParquetWriterConf().getParquetSizeLimit()) {
                    holder.writer.write(record);
                    holder.count += 1;
                }
                else {
                    holder.writer.close();
                    holder = createHolder();
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

        }

    }




}
