package md.hajji.sales.local;

import md.hajji.sales.SalesByCityConfig;
import md.hajji.sales.SalesByCityJobs;
import org.apache.spark.SparkConf;


public class SalesByCity {

    private static final String APP_NAME = "local-sales-by-city";
    private static final String SALES_FILE_LOCATION = "sales.txt";

    public static void main(String[] args) {
        SparkConf sparkConf = SalesByCityConfig.localConf(APP_NAME);
        SalesByCityJobs.run(sparkConf, SALES_FILE_LOCATION);
    }
}
