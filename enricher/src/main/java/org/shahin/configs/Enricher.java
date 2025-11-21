package org.shahin.configs;

public class Enricher {
    private int workerCount;

    public Enricher() {
    }
    public Enricher(int workerCount, String dirPath) {
        this.workerCount = workerCount;
    }
    public int getWorkerCount() {
        return workerCount;
    }
    public void setWorkerCount(int workerCount) {
        this.workerCount = workerCount;
    }

}
