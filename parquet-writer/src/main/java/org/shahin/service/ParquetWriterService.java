package org.shahin.service;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.parquet.hadoop.ParquetWriter;
import org.shahin.configs.ApplicationConfig;
import org.shahin.eventhadnler.KafkaUtils;
import org.shahin.protobuf.NetRecordProto;
import org.shahin.repository.hdfs.IHDFSParquetWriter;
import org.shahin.utils.HDFSFile;
import org.shahin.utils.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;

public class ParquetWriterService  {
    private final ApplicationConfig conf;
    private final IHDFSParquetWriter hdfsWriter;
    private ParquetWriter<NetRecordProto.NetRecord> writer;
    private final Logger logger;
    public ParquetWriterService(ApplicationConfig conf, IHDFSParquetWriter hdfsWriter) {
        this.conf = conf;
        this.hdfsWriter = hdfsWriter;
        this.logger = LoggerFactory.getLogger(ParquetWriterService.class);
    }


    public ParquetWriter<NetRecordProto.NetRecord> getWriter() {
        return writer;
    }
    public void setWriter() {
        this.writer = hdfsWriter.writeToHdfs(conf.getParquetWriterConf().getHdfsDirPath(),conf.getParquetWriterConf().getHdfsUrl());
    }

    public void readAndWrite(){
        Consumer<Long, String> consumer = KafkaUtils.createConsumer(conf.getKafkaConfig().getBootstrapServers(),conf.getKafkaConfig().getgroupId());
        consumer.subscribe(Collections.singletonList(conf.getParquetWriterConf().getNetworkLogsTopic()));
        setWriter();
        logger.info("new parquet file {}", HDFSFile.getHDFSFileName(conf.getParquetWriterConf().getHdfsDirPath()));

        for(;;){
            ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(10000));
            for (ConsumerRecord<Long, String> record : records) {
                try {
                    getWriter().write(Parser.DecodeToProto(record.value()));

                    if (checkIfParquetMustBeClosed()){
                        getWriter().close();
                        try {
                            setWriter();
                        }
                        catch (Exception e) {
                            logger.error(e.getMessage());
                        }                    }
                 }
                catch (Exception e) {
                    logger.error(e.getMessage());
                }
            }

        }
    }
    private Boolean checkIfParquetMustBeClosed() {
        // TODO Add time validator
        long sizeInBytes = getWriter().getDataSize();
        return sizeInBytes / (1024.0 * 1024.0) > conf.getParquetWriterConf().getParquetSizeLimit();
    }
}
