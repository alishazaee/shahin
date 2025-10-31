package org.shahin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.shahin.configs.ApplicationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Stream;
import java.io.Closeable;


public class KafkaIngester implements Closeable {
    private final BlockingQueue<Path> filePathQueue;
    private final BlockingQueue<Path> deletedFilePathQueue;
    private final static Logger logger = LoggerFactory.getLogger(KafkaIngester.class);
    private final int ThreadCount;
    private boolean running = true;
    private final Parser recordParser;
    private final KafkaProducer<byte[],byte[]> producer;
    private final String topic;
    private final Thread[] ingestionThreads;
    private final Thread deleteThread;

    public KafkaIngester(Parser recordParser, ApplicationConfig config) {
        filePathQueue = new LinkedBlockingQueue<>();
        deletedFilePathQueue = new LinkedBlockingQueue<>();
        this.ThreadCount = config.getIngester().getWorkerCount();
        this.recordParser = recordParser;
        producer = new KafkaProducer<>(config.getKafkaConfig().getProperties());
        this.topic = config.getIngester().getTopic();
        ingestionThreads = new Thread[ThreadCount];
        for(int i=0 ; i < ThreadCount ; i++) {
            ingestionThreads[i] = new Thread(this::startIngestion);
        }
        deleteThread = new Thread(this::deleteFile);
    }

    public void start() {
        for(Thread thread : ingestionThreads) {
            thread.start();
        }
        deleteThread.start();
        logger.info("Starting ingestion threads");
    }

    public void sendToKafka(Path filePath,byte[] protoRecord) {
        ProducerRecord<byte[],byte[]> record = new ProducerRecord<>(topic,filePath.getFileName().toString().getBytes(),protoRecord);
        producer.send(record);
        producer.flush();
    }

    public void startIngestion() {
        while (running) {
            try {
                Path file = filePathQueue.take();
                try (Stream<String> stream = Files.lines(file)) {
                    stream.forEach(line -> sendToKafka(file,recordParser.Encode(line)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                if(deletedFilePathQueue.offer(file)){
                    logger.info("Deleted file {}", file.getParent().toString());
                }else {
                    logger.error("Failed to delete file {}", file.getParent().toString());
                    Thread.currentThread().interrupt();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

        }
    }

    public void ingest(Path path){
            if(filePathQueue.offer(path)){
                logger.info("path {} successfully added to the queue.", path);
            } else {
                throw new AssertionError("Adding to path queue failed!");
            }
    }

    public void deleteFile(){
        try{
            Path file = deletedFilePathQueue.take();
            boolean isDeleted = file.toFile().delete();
            if (!isDeleted) {
                logger.error("Failed to delete file {}", file.getParent().toString());
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

    }

    public void close()  {
        running = false;
        try {
            for (Thread thread : ingestionThreads) {
                thread.join();
            }
            deleteThread.join();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError("Interrupted when waiting ingester threads to terminate", e);
        }
        producer.close();
    }

}
