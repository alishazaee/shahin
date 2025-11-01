package org.shahin;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import org.shahin.configs.ApplicationConfig;
import org.shahin.utils.NetRecordParser;
import org.yaml.snakeyaml.Yaml;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;

public class Application {
    private Application(){
    }

    public static void start(ApplicationConfig appConf) {
        PrometheusMeterRegistry prometheusRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);

        NetRecordParser parser = new NetRecordParser();
        KafkaIngester ingester = new KafkaIngester(parser, appConf,prometheusRegistry);
        ingester.start();
        Watcher watcher = new Watcher(ingester::ingest,appConf.getIngester());
        watcher.start();
        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(8383), 0);
            server.createContext("/metrics", httpExchange -> {
                String response = prometheusRegistry.scrape();
                httpExchange.sendResponseHeaders(200, response.getBytes().length);
                httpExchange.getResponseBody().write(response.getBytes());
                httpExchange.close();
            });
            server.start();
        }
        catch (IOException e) {

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
