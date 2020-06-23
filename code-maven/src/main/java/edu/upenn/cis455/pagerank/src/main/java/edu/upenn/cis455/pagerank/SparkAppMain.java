package edu.upenn.cis455.pagerank;

import java.util.*;
import java.util.regex.Pattern;
import java.lang.Math;
import org.apache.spark.sql.*;
import scala.Tuple2;
import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

public final class SparkAppMain {
    private static final Pattern SPACES = Pattern.compile("\\s+");
    private static final String JDBC_URL = "jdbc:mysql://urlinks.crpnw2fluwdp.us-east-1.rds.amazonaws.com:3306/links";
    private static final String MASTER_USER = "admin";
    private static final String MASTER_PSW = "admin4links";

    private static class Sum implements Function2<Double, Double, Double> {
        @Override
        public Double call(Double a, Double b) {
            return a + b;
        }
    }

    public static void createInsertQuery( List<Tuple2<Integer, Double>> output){
        // first clear the page ranks in the table, if there is any
        RDSWrapper.executeUpdate("DELETE FROM links.numrank");

        int count = 0;
        boolean toFlush = true;
        StringBuilder query = null;
        int totalQuery = output.size();
        for (Tuple2<Integer,Double> tuple : output) {
            if (count > 1000000){
                System.out.println("Insert " + count + "records: Total"+ totalQuery);
            }
            count += 1;
            if (toFlush){
                query = new StringBuilder("INSERT IGNORE INTO links.numrank VALUES ");
                toFlush = false;
            }
            String value = null;
            if (count % 10000 == 0 || count == totalQuery){
                // start batch insertion
                value = String.format("(%d, %4.3f) \n", tuple._1(), tuple._2());
                query.append(value);
                RDSWrapper.executeUpdate(query.toString());
                toFlush = true;
            }
            else{
                value = String.format("(%d, %4.3f), \n", tuple._1(), tuple._2());
                query.append(value);
            }

        }
    }

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkAppMain")
                .config("spark.master", "local[8]")
                .config("spark.executor.memory", "16g")
                .config("spark.driver.memory", "16g")
                .config("spark.memory.offHeap.enabled",true)
                .config("spark.memory.offHeap.size","16g")
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        Properties dbProperties = new Properties();
        dbProperties.setProperty("user",MASTER_USER);
        dbProperties.setProperty("password", MASTER_PSW);
        Dataset<Row> df = spark.read().jdbc(JDBC_URL, "(select srcid, tarid from links2.adj) as subset", dbProperties);
        JavaRDD<Row> test =  df.javaRDD();

        JavaPairRDD<Integer, Iterable<Integer>> links = test.mapToPair(s -> {
            return new Tuple2<>((Integer) s.get(0), (Integer) s.get(1));
        }).groupByKey().cache();

        JavaPairRDD<Integer, Double> ranks = links.mapValues(s -> 1.0);
            JavaPairRDD<Integer, Double> last_ranks = ranks;
            float share = 1;
            long rankcount = 0;

            // test for convergence
            while (last_ranks.count() != rankcount || share >= 0.005) {
                last_ranks = ranks;
                JavaPairRDD<Integer, Double> link_values = links.join(ranks).values().flatMapToPair(s -> {
                    List<Tuple2<Integer, Double>> results = new ArrayList<>();
                    for (Integer n : s._1) {
                        results.add(new Tuple2<Integer, Double>(n, s._2() / Iterables.size(s._1())));
                    }
                    return results.iterator();
                });

                ranks = link_values.reduceByKey(new Sum()).mapValues(sum -> 0.15 + sum * 0.85);

                if (ranks.count() == rankcount) {
                    JavaPairRDD<Integer, Tuple2<Double, Double>> rank2 = ranks.join(last_ranks);
                    float notConvergedCount = (float) (rank2.values().map(x -> Math.abs(x._1() - x._2())).filter(x -> x > 0.01).count());
                    float totalCount = (float) (ranks.count());
                    share = notConvergedCount / totalCount;
                }

                System.out.println("after:" + ranks.count());
                rankcount = ranks.count();
            }
            // normalization
            // double T = ranks.map(Tuple2::_2).reduce(Double::sum);
            links.unpersist();
            List<Tuple2<Integer, Double>> output = ranks.collect();
            ranks.unpersist();
            createInsertQuery(output);
            spark.stop();
        }
}
