package org.shahin.utils;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

public class CSVReader {
    private CSVReader() {}

    public static List<File> getFiles(String directoryPath){
        List<File> foundedFiles = new LinkedList<>();
        for(File fileEntry : (new File(directoryPath)).listFiles()){
            if (fileEntry.getName().endsWith(".csv")){
                foundedFiles.add(fileEntry);
            }
        }

        for (File file : foundedFiles) {
            String newFilePath = directoryPath + "/" + file.getName() + ".log";
            File newFile = new File(newFilePath);
            if (!file.renameTo(newFile)){
                throw new RuntimeException("Could not rename " + newFilePath);
            }
            foundedFiles.set(foundedFiles.indexOf(file), newFile);
        }
      return foundedFiles;
    }

    public static String getTime(File file) {
        String fileName =file.getName();
        return fileName.split("-")[0];
    }

    public static int getFileLineCount(String filePath){
        int lineNumber = 1;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line = reader.readLine();
            while((line = reader.readLine()) != null){
                lineNumber++;
            }
        } catch (FileNotFoundException e) {
            e.fillInStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return lineNumber;
    }

}
