package org.shahin;

import org.shahin.configs.ApplicationConfig;

import java.nio.file.Path;


public class Main {
    public static void main(String[] args) {
        String filePath;
        if(args.length > 0) {
            filePath = args[0];
        }else {
            filePath = "";
        }

        ApplicationConfig Config = Application.loadConfig(Path.of(filePath));
        Application.start(Config);

    }
}