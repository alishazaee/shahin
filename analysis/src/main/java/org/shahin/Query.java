package org.shahin;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Query {
    private final SparkSession spark;
    private final String parquetPath;
    private final Logger logger;

    public Query(String sparkMasterNode, String parquetPath) {
        spark = SparkSession.builder()
                .appName("NetFlow Analysis")
                .master(sparkMasterNode)
                .getOrCreate();
        this.parquetPath = parquetPath;
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public void showSchema(){
        logger.info("Showing schema");
        Dataset<Row> df = spark.read()
                .parquet(parquetPath);
        df.printSchema();
        df.show(10);
    }

    public void getTopTotalAttacks(){
        logger.info("Getting top attacks based on date");
        Dataset<Row> df = spark.read().parquet(parquetPath);
        df.createOrReplaceTempView("netflow");

        spark.sql("select date,count(*) AS total from netflow group by date order by total desc LIMIT 10").show();
    }

    public void getTopSrcIpv4Addresses(){
        logger.info("Getting top attacks based on  src ip address");
        Dataset<Row> df = spark.read().parquet(parquetPath);
        df.createOrReplaceTempView("netflow");
        spark.sql("select ipv4_src_addr,count(*) AS total from netflow group by ipv4_src_addr order by total desc LIMIT 10").show();
    }

    public void getInternationalTraffic(){
        logger.info("Getting end to end traffic statistics");
        Dataset<Row> df = spark.read().parquet(parquetPath);
        df.createOrReplaceTempView("netflow");
        spark.sql(
                "SELECT ipv4_src_addr AS src_ip, ipv4_dst_addr AS dst_ip, " +
                        "SUM(out_bytes) AS bytes " +
                        "FROM netflow " +
                        "WHERE ipv4_dst_addr RLIKE '^(?!10\\.|192\\.168\\.|172\\.(1[6-9]|2[0-9]|3[0-1])\\.).*' " +
                        "GROUP BY ipv4_src_addr, ipv4_dst_addr " +
                        "UNION ALL " +
                        "SELECT ipv4_dst_addr AS src_ip, ipv4_src_addr AS dst_ip, SUM(in_bytes) AS bytes " +
                        "FROM netflow " +
                        "WHERE ipv4_src_addr RLIKE '^(?!10\\.|192\\.168\\.|172\\.(1[6-9]|2[0-9]|3[0-1])\\.).*' " +
                        "GROUP BY ipv4_dst_addr, ipv4_src_addr"
        ).show();

    }


}
