package org.shahin.watcher;

import java.io.File;
import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.shahin.utils.CSVReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Watcher implements Runnable {
    private String directory;
    private BlockingQueue<File> fileQueue;
    private final Logger logger;
    public Watcher(String directory,BlockingQueue<File> fileQueue) {
        this.directory = directory;
        this.fileQueue = fileQueue;
        logger =  LoggerFactory.getLogger(Watcher.class);
    }

    public BlockingQueue<File> getFileQueue() {
        return fileQueue;
    }
    public void setFileQueue(BlockingQueue<File> fileQueue) {
        this.fileQueue = fileQueue;
    }
    public String getDirectory() {
        return directory;
    }
    public void setDirectory(String directory) {
        this.directory = directory;
    }

    public void run() {
        for (;;) {
            List<File> files = CSVReader.getFiles(getDirectory());
            for (File file : files){
                logger.info("Found file {} for processing", file.getAbsolutePath());
                try {
                    getFileQueue().put(file);
                }
                catch (Exception e) {
                    logger.error(e.getMessage());

                }

            }
        }
    }
}
