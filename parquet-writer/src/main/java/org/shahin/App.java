package org.shahin;


import org.shahin.service.ParquetWriterService;
import org.shahin.configs.ApplicationConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.shahin.repository.hdfs.HDFS;
import org.yaml.snakeyaml.Yaml;

public class App
{
    public static void main(String[] args)
    {
        ApplicationConfig config = loadConfig(Path.of(args[0]));
        HDFS handler = new HDFS();
        ParquetWriterService service = new ParquetWriterService(config,handler);
        service.readAndWrite();
    }
    public static ApplicationConfig loadConfig(Path yamlPath) {
        ApplicationConfig config;
        try (InputStream stream = Files.newInputStream(yamlPath)) {
            config = new Yaml().loadAs(stream, ApplicationConfig.class);
        } catch (IOException e) {
            throw new AssertionError("Unable to find config file");
        }
        // TODO add validator
        return config;
    }
}
