package org.shahin;


import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import org.apache.commons.lang3.Validate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.Callable;
import java.lang.Thread;

import java.io.IOException;


public class KafkaParquetWriter<T extends Message> implements Closeable {
    private final static Logger logger = LoggerFactory.getLogger(KafkaParquetWriter.class);
    private final int threadCount;
    private final ConsumerWrapper<byte[], byte[]> consumer;
    private final Parser<T> parser;
    private final int retryNumber;
    private final long dataSizeLimit;
    private final int parquetTimeoutInMinute;
    private final Path hdfsBasePath;
    private final String hdfsUrl;

    private final Counter writtenRecords;
    private final Counter writtenBytes;
    private final Counter flushedParquets;
    private final Counter flushedSizeExceededParquets;
    private final Counter flushedTimeExceededParquets;

    private final Parquet.ParquetProperties parquetProperties;
    private final List<Writer> writers;

    public KafkaParquetWriter(KafkaParquetWriterBuilder<T> builder){
        threadCount = builder.threadCount;
        consumer = builder.consumer;
        parquetTimeoutInMinute = builder.parquetTimeoutInMinute;
        dataSizeLimit = builder.dataSizeLimit;
        retryNumber = builder.retryNumber;
        hdfsBasePath = builder.hdfsBasePath;
        parquetProperties = builder.parquetProperties;
        parser = builder.parser;
        hdfsUrl= builder.hdfsUrl;
        MetricRegistry registry = builder.registry;

        writtenRecords = registry.counter(MetricNames.WRITTEN_RECORDS);
        writtenBytes = registry.counter(MetricNames.WRITTEN_BYTES);
        flushedParquets = registry.counter(MetricNames.FLUSHED_PARQUETS);
        flushedTimeExceededParquets = registry.counter(MetricNames.TIMEOUT_PARQUETS);
        flushedSizeExceededParquets = registry.counter(MetricNames.SIZE_EXCEED_PARQUETS);

        writers = new ArrayList<>();

    }

    public void start(){
        consumer.start();
        for(int i =0; i< threadCount; i++){
            Writer writer = new Writer();
            Thread thread = new Thread(writer);
            writers.add(writer);
            thread.start();
            thread.setName("writer-" + i);
        }
        logger.info("Starting kafka writer");
    }

    @Override
    public void close() throws IOException {
        for(Writer writer : writers){
            writer.close();
            logger.error("Closing kafka writers");
        }
    }

    public class Writer implements Runnable,Closeable{

        public Parquet<T> currentParquet;
        private  boolean running = true;
        private List<PartitionOffset> writtenOffsets;
        private Path tempFilePath;

        public Writer() {
            writtenOffsets = new ArrayList<>();
        }

        public boolean isCurrentParquetFull(){
            return currentParquet.getFileSize() >= dataSizeLimit;
        }

        public boolean isCurrentFileTimeout(){
            Date currentDate = new Date();
            long timeDifference = currentDate.getTime() - currentParquet.getCreationDate().getTime();

            long TimeOutInMillis = parquetTimeoutInMinute * 60L * 1000;

            return timeDifference > TimeOutInMillis;

        }

        @Override
        public void run() {
            while(running){
                try {
                    if (currentParquet != null && isCurrentFileTimeout()){
                        logger.info("Current file timeout exceeded "+ tempFilePath);
                        CloseTheCurrentParquet();
                        flushedTimeExceededParquets.inc();
                    }

                    ConsumerRecord<?, byte[]> record = consumer.poll();
                    if (record == null) {
                        continue;
                    } else if (currentParquet == null) {
                        tempFilePath = new Path(hdfsBasePath, "/temp/"+ UUID.randomUUID()+".parquet");
                        currentParquet = new Parquet<T>(parquetProperties, tempFilePath);
                    }

                    writtenOffsets.add(new PartitionOffset(record.offset(),record.partition()));

                    T log;
                    try {
                        log = parser.parseFrom(record.value());

                    } catch (InvalidProtocolBufferException e) {
                        throw new IllegalStateException("the parser can not parse the log" + e.getMessage());
                    }
                    retryOperation(() -> {
                        currentParquet.write(log);
                        writtenRecords.inc();
                        writtenBytes.inc(record.serializedValueSize());
                        return null;
                    }, retryNumber);

                    if (isCurrentParquetFull()){
                        logger.info("Current file size exceeded "+ tempFilePath);
                        CloseTheCurrentParquet();
                        flushedSizeExceededParquets.inc();
                    }
                }
                catch (IOException e){
                    running = false;
                    logger.error("try to shutdown the parquet writer");
                    throw new IllegalStateException("Unexpected exception occurred", e.getCause());
                }
                catch (Exception e){
                    running = false;
                    logger.error("try to shutdown the parquet writer " + e.getMessage());
                    throw new RuntimeException("Unexpected exception occurred", e.getCause());
                }
            }

        }

        public void renameClosedFile() throws IOException {
            FileSystem fs = FileSystem.get(parquetProperties.getHadoopConf());
            String year = String.valueOf(Calendar.getInstance().get(Calendar.YEAR));
            String month = String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1);
            String day = String.valueOf(Calendar.getInstance().get(Calendar.DAY_OF_MONTH));
            Path parentDir = new Path(hdfsUrl+ "/" + year + "/" + month + "/" + day);
            if (!fs.exists(parentDir)) {
                if (!fs.mkdirs(parentDir)) {
                    throw new IOException("Failed to create parent directories: " + parentDir);
                }
            }

            Path newHdfsPath = new Path(hdfsUrl + "/" + year + "/" + month + "/" + day + "/" + UUID.randomUUID() + ".parquet");

            if (fs.exists(tempFilePath)) {
                boolean isMoved = fs.rename(tempFilePath, newHdfsPath);
                if (isMoved) {
                    logger.info("File moved from {} to {}", tempFilePath, newHdfsPath);
                } else {
                    logger.error("Failed to move file from {} to {}", tempFilePath, newHdfsPath);
                    throw new IOException("Failed to move file");
                }
            } else {
                logger.error("Source file does not exist: {}", tempFilePath);
                throw new IOException("Source file does not exist");
            }
            flushedParquets.inc();
        }

        public void CloseTheCurrentParquet() throws IOException {
            currentParquet.close();
            logger.info("Closing the current parquet" + tempFilePath.toString());

            renameClosedFile();

            for (PartitionOffset offset : writtenOffsets){
                consumer.ack(offset);
            }
            writtenOffsets.clear();
            // after closing the old file, we create a new parquet
            currentParquet = null;
        }

        @Override
        public void close() throws IOException {
            currentParquet.close();
        }
    }

    private static <V> void retryOperation(Callable<V> operation, int maxAttempts) throws IOException {
        for (int i = 0; i < maxAttempts; i++) {
            try {
                operation.call();
                return;
            }
            catch (IOException e){
                try {
                    Thread.sleep(500);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                }
                logger.error("error while retrying operation " + operation, e);
                if (i == maxAttempts - 1) {
                    throw new IOException("error while retrying operation " + operation + e.getMessage());
                }
            }
            catch (Exception e) {
                logger.error("something unexpected happened, error while retrying operation " + operation, e);
                throw new RuntimeException(e);
            }
        }
    }

    public static class KafkaParquetWriterBuilder<T extends Message> {
        private String topic;
        private int threadCount;
        private ConsumerWrapper<byte[],byte[]> consumer;
        private Properties properties;
        private int retryNumber;
        private long dataSizeLimit;
        private int parquetTimeoutInMinute;
        private Path hdfsBasePath;
        private long blockSize;
        private int pageSize;
        private Class<T> protoClass;
        private Parquet.ParquetProperties parquetProperties;
        private String hdfsUrl;
        private Parser<T> parser;
        private MetricRegistry registry;

        public KafkaParquetWriterBuilder<T> setTopic(String topic) {
            Validate.notNull(topic, "topic cannot be null");
            this.topic = topic;
            return this;
        }
        public KafkaParquetWriterBuilder<T> setThreadCount(int threadCount) {
            Validate.isTrue(threadCount > 0, "thread count must be greater than 0");
            this.threadCount = threadCount;
            return this;
        }
        public KafkaParquetWriterBuilder<T> setRetryNumber(int retryNumber) {
            Validate.isTrue(retryNumber > 0, "retry number must be greater than 0");
            this.retryNumber = retryNumber;
            return this;
        }
        public KafkaParquetWriterBuilder<T> setDataSizeLimit(long dataSizeLimit) {
            Validate.isTrue(dataSizeLimit > 10, "dataSizeLimit must be greater than 50 MB");
            this.dataSizeLimit = dataSizeLimit;
            return this;
        }

        public KafkaParquetWriterBuilder<T> setProperties(Properties properties) {
            this.properties = properties;
            return this;
        }

        public KafkaParquetWriterBuilder<T> setHdfsBasePath(String hdfsUrl, String hdfsDirectory) {
            Validate.notNull(hdfsUrl, "hdfsBasePath can not be null");
            Validate.notNull(hdfsDirectory, "hdfsDirectory can not be null");
            this.hdfsUrl = hdfsUrl;
            this.hdfsBasePath =new Path(hdfsUrl + hdfsDirectory);
            return this;
        }

        public KafkaParquetWriterBuilder<T> setBlockSize(long blockSize) {
            Validate.isTrue(blockSize > 0, "blockSize must be greater than 0");
            this.blockSize = blockSize;
            return this;
        }

        public KafkaParquetWriterBuilder<T> setProtoClass(Class<T> protoClass) {
            this.protoClass = protoClass;
            return this;
        }

        public KafkaParquetWriterBuilder<T> setPageSize(int pageSize) {
            Validate.isTrue(pageSize > 0, "pageSize must be greater than 0");
            this.pageSize = pageSize;
            return this;
        }

        public KafkaParquetWriterBuilder<T> setParquetTimeoutInMinute(int parquetTimeoutInMinute) {
            Validate.notNull(parquetTimeoutInMinute, "parquetTimeoutInMinute must be greater than 0");
            this.parquetTimeoutInMinute = parquetTimeoutInMinute;
            return this;
        }


        public KafkaParquetWriterBuilder<T> setParser(Parser<T> parser) {
            Validate.notNull(parser);
            this.parser = parser;
            return this;
        }

        public KafkaParquetWriterBuilder<T> setRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }

        public KafkaParquetWriter<T> build() {
            Configuration config = new Configuration();
            config.set("fs.defaultFS", hdfsUrl);
            parquetProperties = new Parquet.ParquetProperties<T>();
            parquetProperties.setBlockSize(blockSize);
            parquetProperties.setPageSize(pageSize);
            parquetProperties.setHadoopConf(config);
            parquetProperties.setProtoClass(protoClass);
            parquetProperties.setCompressionCodecName(CompressionCodecName.SNAPPY);
            consumer = new ConsumerWrapper.Builder<byte[],byte[]>().withCapacity(100).withProperties(properties).build();
            consumer.subscribe(topic);
            return new KafkaParquetWriter<>(this);
        }

    }

}
