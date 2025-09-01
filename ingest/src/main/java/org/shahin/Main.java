package org.shahin;

import org.shahin.configs.ApplicationConfig;

import java.io.File;
import java.nio.file.Path;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) throws InterruptedException {
        String filePath;
        try {
             filePath = args[0];
        }
        catch (ArrayIndexOutOfBoundsException e ) {
            filePath = "";
        }


        BlockingQueue<File> queue = new LinkedBlockingQueue<>();
        ApplicationConfig Config = Application.loadConfig(Path.of(filePath));
        Application.Start(Config,queue);

    }
}