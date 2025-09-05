package org.shahin;

import org.apache.kafka.clients.consumer.Consumer;
import org.shahin.eventhadnler.KafkaUtils;
import org.shahin.protobuf.NetRecordProto;
import org.shahin.configs.ApplicationConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.shahin.service.AckRecord;
import org.shahin.service.KafkaRecord;
import org.shahin.service.Reader;
import org.shahin.service.Writer;
import org.yaml.snakeyaml.Yaml;

public class App
{

    public static void Start(ApplicationConfig config) {

        BlockingQueue<KafkaRecord<NetRecordProto.NetRecord>> Records = new LinkedBlockingQueue<>(config.getQueueCapacity());

        BlockingQueue<AckRecord> Acks = new LinkedBlockingQueue<>(config.getQueueCapacity()*3);


        Reader reader =new Reader(config,Records,Acks);
        reader.Start();

        ExecutorService writerExecutor = Executors.newFixedThreadPool(config.getParquetWriterConf().getWorkerNumber());
        for(int i =0; i< config.getParquetWriterConf().getWorkerNumber(); i++){
            writerExecutor.execute(new Writer(config,Records,Acks));
        }

    }


    public static void main(String[] args)
    {
        String filePath;
        try {
            filePath = args[0];
        }
        catch (ArrayIndexOutOfBoundsException e ) {
            filePath = "";
        }

        ApplicationConfig config = loadConfig(Path.of(filePath));
        Start(config);
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
