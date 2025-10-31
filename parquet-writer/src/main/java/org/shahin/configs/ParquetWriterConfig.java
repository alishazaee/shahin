package org.shahin.configs;

public class ParquetWriterConfig {
    String networkLogsTopic;
    String hdfsDirPath;
    long parquetSizeLimit;
    String hdfsUrl;
    int workerNumber;
    int pageSize;
    long blockSize;
    int parquetTimeout;


    private ParquetWriterConfig() {
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

    public long getParquetSizeLimit() {
        return parquetSizeLimit;
    }

    public int getPageSize(){
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public long getBlockSize() {
        return blockSize;
    }

    public void setParquetTimeout(int parquetTimeout) {
        this.parquetTimeout = parquetTimeout;
    }

    public int getParquetTimeout() {
        return parquetTimeout;
    }


    public void setBlockSize(long blockSize) {
        this.blockSize = blockSize;
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

    public void setParquetSizeLimit(long parquetSizeLimit) {
        this.parquetSizeLimit = parquetSizeLimit;
    }

    public void setHdfsUrl(String hdfsUrl) {
        this.hdfsUrl = hdfsUrl;
    }

    public void setWorkerNumber(int workerNumber) {
        this.workerNumber = workerNumber;
    }
}
