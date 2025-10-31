package org.shahin;

import com.codahale.metrics.MetricRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.HTTPServer;
import org.shahin.configs.ApplicationConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.shahin.protobuf.NetRecordProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class App
{

    public static void Start(ApplicationConfig config)  {
        Logger logger = LoggerFactory.getLogger(KafkaParquetWriter.class);
        MetricRegistry metricRegistry = new MetricRegistry();
        KafkaParquetWriter<NetRecordProto.NetRecord> parquetWriter = new KafkaParquetWriter.KafkaParquetWriterBuilder<NetRecordProto.NetRecord>()
                .setBlockSize(config.getParquetWriterConf().getBlockSize())
                .setDataSizeLimit(config.getParquetWriterConf().getParquetSizeLimit())
                .setPageSize(config.getParquetWriterConf().getPageSize())
                .setTopic(config.getParquetWriterConf().getNetworkLogsTopic())
                .setRetryNumber(3)
                .setThreadCount(config.getParquetWriterConf().getWorkerNumber())
                .setParquetTimeoutInMinute(config.getParquetWriterConf().getParquetTimeout())
                .setParser(NetRecordProto.NetRecord.parser())
                .setHdfsBasePath(config.getParquetWriterConf().getHdfsUrl(), config.getParquetWriterConf().getHdfsDirPath())
                .setProtoClass(NetRecordProto.NetRecord.class)
                .setProperties(config.getKafkaConfig().getProperties())
                .setRegistry(metricRegistry)
                .build();
        parquetWriter.start();
        CollectorRegistry prometheusRegistry = new CollectorRegistry();
        prometheusRegistry.register(new DropwizardExports(metricRegistry));
        try {
             new HTTPServer.Builder()
                    .withPort(config.getPrometheusPort())
                    .withRegistry(prometheusRegistry)
                    .build();
        }
        catch (IOException e) {
            logger.error("Unable to start prometheus server ", e.getMessage());
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
