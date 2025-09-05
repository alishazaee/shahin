package org.shahin;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        Query query = new Query("local[1]","hdfs://namenode:9000/data/2025/9/2/*.parquet");
        query.showSchema();
        query.getTopTotalAttacks();
        query.getTopSrcIpv4Addresses();
        query.getInternationalTraffic();

    }
}