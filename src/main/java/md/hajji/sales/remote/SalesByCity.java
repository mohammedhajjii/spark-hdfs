package md.hajji.sales.remote;

import md.hajji.sales.SalesByCityConfig;
import md.hajji.sales.SalesByCityJobs;
import org.apache.spark.SparkConf;

public class SalesByCity {

    private static final String APP_NAME = "remote-sales-by-city";
    private static final String SALES_FILE_LOCATION = "hdfs://namenode:8020/input/sales.txt";

    public static void main(String[] args) {
        SparkConf sparkConf = SalesByCityConfig.remoteConf(APP_NAME);
        SalesByCityJobs.run(sparkConf, SALES_FILE_LOCATION);
    }
}
