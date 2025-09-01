package org.shahin;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.shahin.configs.ApplicationConfig;
import org.shahin.eventhandler.KafkaUtils;
import org.shahin.utils.CSVReader;
import org.shahin.watcher.Watcher;
import org.shahin.watcher.Worker;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class Application {
    private Application(){
    }

    public static void Start(ApplicationConfig appConf, BlockingQueue<File> fileQueue) throws InterruptedException {
        KafkaProducer<Long, String> kafkaPublisher =  KafkaUtils.createProducer(
                appConf.getIngester().getClientId(),
                appConf.getKafkaConfig());

        Watcher watcher = new Watcher(
                appConf.getIngester().getDirPath(),
                fileQueue
        );
        Thread watcherThread = new Thread(watcher);
        watcherThread.start();

        Worker worker = new Worker(fileQueue,kafkaPublisher);
        ExecutorService pool = Executors.newFixedThreadPool(appConf.getIngester().getWorkerCount());
        pool.execute(worker);
    }

    public static ApplicationConfig loadConfig(Path yamlPath) {
        ApplicationConfig config;
        if (!yamlPath.toFile().exists()) {
            yamlPath = Path.of("src/main/resources/app.yaml");
        }
        try (InputStream stream = Files.newInputStream(yamlPath)) {
            config = new Yaml().loadAs(stream, ApplicationConfig.class);
        } catch (IOException e) {
            throw new AssertionError("Unable to find config file");
        }
        // TODO add validator
        return config;
    }

}
