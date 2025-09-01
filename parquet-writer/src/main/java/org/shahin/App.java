package org.shahin;


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

import org.shahin.service.Reader;
import org.shahin.service.Writer;
import org.yaml.snakeyaml.Yaml;

public class App
{

    public static void Start(ApplicationConfig config) {
        BlockingQueue<NetRecordProto.NetRecord> queue = new LinkedBlockingQueue<>(config.getQueueCapacity());
        Writer writer = new Writer(config,queue);
        ExecutorService writerExecutor = Executors.newFixedThreadPool(config.getParquetWriterConf().getWorkerNumber());
        writerExecutor.execute(writer);


        Reader reader = new Reader(config,queue);
        ExecutorService readerExecutor = Executors.newFixedThreadPool(config.getParquetWriterConf().getWorkerNumber());
        readerExecutor.execute(reader);
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
//        HDFS handler = new HDFS();
//        ParquetWriterService service = new ParquetWriterService(config,handler);
//        service.readAndWrite();
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
