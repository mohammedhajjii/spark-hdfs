package md.hajji.sales;

import org.apache.spark.SparkConf;

public class SalesByCityConfig {

    /**
     * create and initialize SparkConf for local use case
     * @param appName : the application name
     * @return : SparkConf instance
     */
    public static SparkConf localConf(String appName){
        return new SparkConf()
                .setAppName(appName)
                .setMaster("local[*]");
    }

    /**
     * create and initialize `SparkConf` for remote use case
     * @param appName : the application name
     * @return : SparkConf instance
     */
    public static SparkConf remoteConf(String appName){
        return new SparkConf()
                .setAppName(appName);
    }
}
