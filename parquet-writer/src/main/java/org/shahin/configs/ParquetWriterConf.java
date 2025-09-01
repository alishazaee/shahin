package org.shahin.configs;

public class ParquetWriterConf {
    String networkLogsTopic;
    String hdfsDirPath;
    double parquetSizeLimit;
    String hdfsUrl;
    int workerNumber;

    private ParquetWriterConf() {
    }

    public String getNetworkLogsTopic() {
        return networkLogsTopic;
    }

    public String getHdfsUrl(){
        return hdfsUrl;
    }

    public String getHdfsDirPath() {
        return hdfsDirPath;
    }
    public double getParquetSizeLimit() {
        return parquetSizeLimit;
    }

    public int getWorkerNumber() {
        return workerNumber;
    }

    public void setNetworkLogsTopic(String networkLogsTopic) {
        this.networkLogsTopic = networkLogsTopic;
    }

    public void setHdfsDirPath(String hdfsDirPath) {
        this.hdfsDirPath = hdfsDirPath;
    }

    public void setParquetSizeLimit(double parquetSizeLimit) {
        this.parquetSizeLimit = parquetSizeLimit;
    }

    public void setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    public void setWorkerNumber(int workerNumber) {
        this.workerNumber = workerNumber;
    }
}
