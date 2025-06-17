package org.shahin.configs;

public class ParquetWriterConf {
    String networkLogsTopic;
    String hdfsDirPath;
    double parquetSizeLimit;
    String hdfsUrl;
    private ParquetWriterConf() {
    }
    public String getNetworkLogsTopic() {
        return networkLogsTopic;
    }
    public void setNetworkLogsTopic(String networkLogsTopic) {
        this.networkLogsTopic = networkLogsTopic;
    }
    public String getHdfsUrl(){
        return hdfsUrl;
    }
    public void setHdfsUrl(String hdfsUrl){
        this.hdfsUrl = hdfsUrl;
    }
    public String getHdfsDirPath() {
        return hdfsDirPath;
    }
    public void setHdfsDirPath(String hdfsDirPath) {
        this.hdfsDirPath = hdfsDirPath;
    }
    public double getParquetSizeLimit() {
        return parquetSizeLimit;
    }
    public void setParquetSizeLimit(double parquetSizeLimit) {
        this.parquetSizeLimit = parquetSizeLimit;
    }


}
