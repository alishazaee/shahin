package org.shahin.watcher;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.shahin.utils.CSVReader;
import org.shahin.utils.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.BlockingQueue;

public class Worker implements Runnable {
    private  BlockingQueue<File> fileQueue;
    private  KafkaProducer<Long,String> producer;
    private final Logger logger;
    public Worker(BlockingQueue<File> fileQueue , KafkaProducer<Long,String> producer) {
        this.fileQueue = fileQueue;
        this.producer = producer;
        this.logger = LoggerFactory.getLogger(Worker.class);
    }

    public KafkaProducer<Long,String> getProducer() {
        return producer;
    }
    public void setProducer(KafkaProducer<Long,String> producer) {
        this.producer = producer;
    }
    public BlockingQueue<File> getFileQueue() {
        return fileQueue;
    }
    public void setFileQueue(BlockingQueue<File> fileQueue) {
        this.fileQueue = fileQueue;
    }

    @Override
    public void run() {
        for (;;) {
            File file;
            try {
                file = getFileQueue().take();
                logger.info("Processing file: {}, size: {}", file.getAbsoluteFile(), file.length());

            } catch (InterruptedException e) {
                logger.error(e.getMessage());
                throw new RuntimeException(e);
            }
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                String line;
                logger.info(String.format("Reading file: %s", file.getName()));

                while ((line = reader.readLine()) != null) {
                       try {
                           String Time = CSVReader.getTime(file);
                           String EncodedStr = Parser.EncodeNetRecord(line.split(","), Time);
                           ProducerRecord<Long, String> record = new ProducerRecord<>("network-logs", Long.parseLong(Time.split("_")[0]), EncodedStr);
                           getProducer().send(record,(metadata, exception) -> {
                               if( exception != null ) {
                                   logger.error(exception.getMessage());
                               }
                           });
                       }catch (NumberFormatException e) {
                        logger.info("columns are {}",e.getMessage());
                       }
                       catch (Exception e) {
                           logger.error(e.getMessage());
                       }

                }

            } catch (IOException e) {
                logger.error(e.getMessage());
                throw new RuntimeException("Error reading file: " + file.getName(), e);
            }
            catch (Exception e) {
                logger.error(e.getMessage());
            }
            getProducer().flush();
            logger.info(String.format("Finished Processing: %s", file.getName()));
        }
    }


}
