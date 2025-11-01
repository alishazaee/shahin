package org.shahin;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.shahin.configs.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Counter;

public class KafkaIngester  {
    private final BlockingQueue<Path> filePathQueue;
    private final BlockingQueue<Path> deletedFilePathQueue;
    private final static Logger logger = LoggerFactory.getLogger(KafkaIngester.class);
    private final int ThreadCount;
    private volatile boolean running = true;
    private final Parser recordParser;
    private final KafkaProducer<byte[],byte[]> producer;
    private final String topic;
    private final Thread[] ingestionThreads;
    private final Thread deleteThread;
    private Counter finishedFiles;
    private  Counter deletedFiles;
    private final Map<String, AtomicLong> fileReadDuration = new ConcurrentHashMap<>();

    public KafkaIngester(Parser recordParser, ApplicationConfig config, MeterRegistry meterRegistery) {
        filePathQueue = new LinkedBlockingQueue<>();
        deletedFilePathQueue = new LinkedBlockingQueue<>();
        this.ThreadCount = config.getIngester().getWorkerCount();
        this.recordParser = recordParser;
        producer = new KafkaProducer<>(config.getKafkaConfig().getProperties());
        this.topic = config.getIngester().getTopic();
        ingestionThreads = new Thread[ThreadCount];
        for(int i=0 ; i < ThreadCount ; i++) {
            ingestionThreads[i] = new Thread(this::startIngestion);
            ingestionThreads[i].setName("ingestion-"+i);
            AtomicLong threadDelay =new AtomicLong(0);
            fileReadDuration.putIfAbsent("ingestion-"+i, threadDelay);
            Gauge.builder("file_read_delay",threadDelay, AtomicLong::get).tags("thread", String.valueOf(i)).register(meterRegistery);
        }
        deleteThread = new Thread(this::deleteFile);

        finishedFiles = meterRegistery.counter("finishedFiles");
        deletedFiles = meterRegistery.counter("deletedFiles");

    }

    public void start() {
        for(Thread thread : ingestionThreads) {
            thread.start();
        }
        deleteThread.start();
        logger.info("Starting ingestion threads");
    }

    public void sendToKafka(Path filePath,List<byte[]> protoRecords) {
        for(byte[] protoRecord : protoRecords) {
            ProducerRecord<byte[],byte[]> record = new ProducerRecord<>(topic,filePath.getFileName().toString().getBytes(),protoRecord);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    logger.error("Failed to send record for file {}", filePath, exception);
                } else {
                    logger.debug("Sent record → topic={} partition={} offset={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });
        }
        producer.flush();
    }

    public void registerThreadDelay(long delay){
        String threadName = Thread.currentThread().getName();
        fileReadDuration.computeIfAbsent(threadName, k -> new AtomicLong(0)).getAndSet(delay);
    }

    public void startIngestion() {
        while (running) {
            try {
                Path file = filePathQueue.take();
                Long startTime = System.currentTimeMillis();
                try (var lines = Files.lines(file)) {

                    List<byte[]> batchRecords = new ArrayList<>();

                    lines.forEach(line -> {
                        Optional<byte[]> record = recordParser.encode(line);
                        record.ifPresent(batchRecords::add);

                        if (batchRecords.size() >= 1000) {
                            sendToKafka(file, batchRecords);
                            batchRecords.clear();
                        }
                    });
                    if (!batchRecords.isEmpty()) {
                        sendToKafka(file, batchRecords);
                    }
                    Long endTime = System.currentTimeMillis();
                    registerThreadDelay((endTime - startTime)/1000);

                    finishedFiles.increment();

                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if(deletedFilePathQueue.offer(file)){
                    logger.info("Deleted file {}", file);
                }else {
                    logger.error("Failed to delete file {}", file.getParent().toString());
                    close();
                }
            }
            catch (InterruptedException e) {
                close();
            }

        }
    }

    public void ingest(Path path){
            if(filePathQueue.offer(path)){
                logger.info("path {} successfully added to the queue.", path);
            } else {
                close();
                throw new AssertionError("Adding to path queue failed!");
            }
    }

    public void deleteFile(){
        while(running){
            try{
                Path file = deletedFilePathQueue.take();
                boolean isDeleted = file.toFile().delete();
                if (!isDeleted) {
                    logger.error("Failed to delete file {}", file);
                }
                deletedFiles.increment();
            } catch (InterruptedException e) {
                close();
                if(!running){
                    return;
                }
            }
        }

    }

    public void close()  {
        logger.error("try to close the kafka ingester");
        running = false;
        for(Thread thread : ingestionThreads) {
            thread.interrupt();
        }
        deleteThread.interrupt();

        try {
            for (Thread thread : ingestionThreads) {
                thread.join();
            }
            deleteThread.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            close();
            throw new AssertionError("Interrupted when waiting ingester threads to terminate", e);
        }
        producer.close();
    }

}
