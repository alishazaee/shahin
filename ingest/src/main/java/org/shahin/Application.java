package org.shahin;
import org.shahin.configs.ApplicationConfig;
import org.shahin.utils.NetRecordParser;
import org.yaml.snakeyaml.Yaml;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class Application {
    private Application(){
    }

    public static void start(ApplicationConfig appConf) {
        NetRecordParser parser = new NetRecordParser();
        try(KafkaIngester ingester = new KafkaIngester(parser, appConf)){
            ingester.start();
            Watcher watcher = new Watcher(ingester::ingest,appConf.getIngester());
            watcher.start();
        }
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
