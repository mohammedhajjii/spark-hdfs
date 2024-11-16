package md.hajji.sales.remote;

import md.hajji.sales.SalesByCityConfig;
import md.hajji.sales.SalesByCityJobs;
import org.apache.spark.SparkConf;


public class SalesByCityAndYear {

    private static final String APP_NAME = "Sales-By-City-And-Year";
    private static final String SALES_FILE_LOCATION = "hdfs://namenode:8020/input/sales.txt";

    public static void main(String[] args) {

        // receive year argument:
        int year = Integer.parseInt(args[0]);

        //initialize spark configs:
        SparkConf sparkConf = SalesByCityConfig.remoteConf(APP_NAME);
        // run Job:
        SalesByCityJobs.run(sparkConf, SALES_FILE_LOCATION, year);
    }
}
