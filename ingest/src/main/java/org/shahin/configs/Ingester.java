package org.shahin.configs;

public class Ingester {
    private int workerCount;
    private String dirPath;
    private String clientId;
    public Ingester() {

    }
    public Ingester(int workerCount, String dirPath) {
        this.workerCount = workerCount;
        this.dirPath = dirPath;
    }
    public int getWorkerCount() {
        return workerCount;
    }
    public String getDirPath() {
        return dirPath;
    }
    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }
    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }
    public String getClientId() {
        return clientId;
    }
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }
}
