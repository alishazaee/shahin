package org.shahin;

import org.apache.avro.protobuf.ProtobufData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Date;

public class Parquet<T> implements Closeable {
    private static final Logger logger = LoggerFactory.getLogger(Parquet.class);
    private Path hdfsPath;
    private ParquetWriter<T> writer;
    private Date creationDate;
    private int recordsWritten = 0;
    private Class<T> protoClass;

    public Parquet(ParquetProperties<T> properties, Path hdfsPath) throws IOException {
        this.creationDate = new Date();
        this.hdfsPath = hdfsPath;
        this.protoClass = properties.getProtoClass();

        this.writer = AvroParquetWriter.<T>builder(hdfsPath)
                .withSchema(ProtobufData.get().getSchema(protoClass))
                .withDataModel(ProtobufData.get())
                .withConf(properties.getHadoopConf())
                .withPageSize(properties.getPageSize())
                .withRowGroupSize(properties.getBlockSize())
                .withCompressionCodec(properties.getCompressionCodecName())
                .build();
    }

    public void write(T log) throws IOException {
        writer.write(log);
        recordsWritten++;
    }

    public Date getCreationDate() {
        return creationDate;
    }

    public long getFileSize() {
        return writer.getDataSize() / (1024 * 1024);
    }

    public int getRecordsWritten() {
        return recordsWritten;
    }

    @Override
    public void close() throws IOException {
        writer.close();
        logger.info("Parquet writer closed" + " filename :"+ hdfsPath.toString());
    }

    public static class ParquetProperties<T> {
        private int pageSize = 1048576;
        private long blockSize = 134217728; 
        private CompressionCodecName compressionCodecName = CompressionCodecName.SNAPPY;
        private Class<T> protoClass;
        private Configuration hadoopConf;

        public int getPageSize() {
            return pageSize;
        }

        public void setPageSize(int pageSize) {
            this.pageSize = pageSize;
        }

        public long getBlockSize() {
            return blockSize;
        }

        public void setBlockSize(long blockSize) {
            this.blockSize = blockSize;
        }

        public CompressionCodecName getCompressionCodecName() {
            return compressionCodecName;
        }

        public void setCompressionCodecName(CompressionCodecName compressionCodecName) {
            this.compressionCodecName = compressionCodecName;
        }

        public Class<T> getProtoClass() {
            return protoClass;
        }

        public void setProtoClass(Class<T> protoClass) {
            this.protoClass = protoClass;
        }

        public Configuration getHadoopConf() {
            return hadoopConf;
        }

        public void setHadoopConf(Configuration hadoopConf) {
            this.hadoopConf = hadoopConf;
        }
    }
}
