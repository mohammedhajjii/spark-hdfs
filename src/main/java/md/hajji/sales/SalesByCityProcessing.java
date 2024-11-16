package md.hajji.sales;

import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class SalesByCityProcessing{

    /**
     * date formatter used in filter stage to cast:
     */
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("dd-MM-yyyy");


    /**
     * create pairs (tuples) for given line of file
     * @param line: the corresponding line in txt file
     * @return : A Pair of City as key and Price as value
     */
    public static Tuple2<String, Double> mapToCityPricePair(String line){
        String[] tokens = line.split(" ");
        return new Tuple2<>(tokens[1], Double.parseDouble(tokens[3]));
    }


    /**
     * Filter a record (line in txt file) by year:
     * @param line: the corresponding line in txt file
     * @param year: the year argument
     * @return : keep only line those matching the given year
     */
    public static boolean filterSalesByYear(String line, Broadcast<Integer> year) {
        return LocalDate
                .parse(line.split(" ")[0], FORMATTER)
                .getYear() == year.value();

    }


    /**
     * print Tuple details key and value:
     * @param tuple: A Pair of City and Price
     */
    public static void logResults(Tuple2<String, Double> tuple){
        System.out.println("City: " + tuple._1 + " ===>> Total Prices : " + tuple._2);
    }

}
