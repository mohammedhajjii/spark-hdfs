package md.hajji.sales;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;

public class SalesByCityJobs {

    /**
     * run a job for calculating total prices by city
     * @param conf : SparkConf instance
     * @param salesFileLocation : The location of sales.txt file
     */
    public static void run(SparkConf conf, String salesFileLocation) {
        // create JavaSparkContext instance:
        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)){


            sparkContext.setLogLevel("WARN");

            // start performing actions:

            // read sales file:
            sparkContext.textFile(salesFileLocation)
                    // create Tuples city and prices:
                    .mapToPair(SalesByCityProcessing::mapToCityPricePair)
                    // calculate sum of prices by city:
                    .reduceByKey(Double::sum)
                    // collect results so the spark-master can collect results
                    // returned by workers:
                    .collect()
                    // print results:
                    .forEach(SalesByCityProcessing::logResults);

        }

    }

    /**
     * run a job for calculating total prices by city for a specific year
     * @param conf : SparkConf instance
     * @param salesFileLocation : The location of sales.txt file
     * @param year : year value
     */
    public static void run(SparkConf conf, String salesFileLocation, int year) {

        // open JavaSparkContext:
        try (JavaSparkContext sparkContext = new JavaSparkContext(conf)) {

            // set Log level:
            sparkContext.setLogLevel("WARN");

            // share year argument with workers:
            Broadcast<Integer> yearBroadcast = sparkContext.broadcast(year);

            // create Java Pai RDD with key is city
            // name and value is total price of sales:
            JavaPairRDD<String, Double> salesByCityAndYear =
                    // read sales file:
                    sparkContext.textFile(salesFileLocation)
                            // keep only record that match given year:
                            .filter(record ->
                                    SalesByCityProcessing.filterSalesByYear(record, yearBroadcast))
                            // create Pairs RDD:
                            .mapToPair(SalesByCityProcessing::mapToCityPricePair)
                            // sum prices by city name:
                            .reduceByKey(Double::sum);

            // if one or more record match given year:
            if (salesByCityAndYear.count() > 0){
                System.out.println("Total sales by city for year: " + year);
                salesByCityAndYear.collect()
                        .forEach(SalesByCityProcessing::logResults);
            }
            else
                System.out.println("No Result found for given year: " + year);

        }
    }
}
