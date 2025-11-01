package org.shahin;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.shahin.configs.Ingester;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.util.function.Consumer;

public class Watcher  {
    Consumer<Path> pathConsumer;
    FileAlterationMonitor monitor;
    private final static Logger logger = LoggerFactory.getLogger(Watcher.class);

    public Watcher(Consumer<Path> pathConsumer, Ingester ingesterConfig) {
        this.pathConsumer = pathConsumer;
        FileAlterationObserver observer = new FileAlterationObserver(ingesterConfig.getDirPath());
        this.monitor = new FileAlterationMonitor(ingesterConfig.getPollInterval());
        FileAlterationListener listener = new FileAlterationListenerAdaptor() {
            @Override
            public void onFileCreate(File file) {
                if (file.getName().toLowerCase().endsWith(".csv")) {
                    logger.info("Watcher for CSV file {}", file.getAbsolutePath() );
                    pathConsumer.accept(file.toPath());
                }
            }

        };
        observer.addListener(listener);
        monitor.addObserver(observer);
    }

    public void start() {
        try {
            monitor.start();
        } catch (Exception e) {
            logger.error(e.getMessage());
            close();
            throw new AssertionError(e);
        }
    }


    public void close() {
        if (monitor != null) {
            try {
                monitor.stop();
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }

        }
    }
}
