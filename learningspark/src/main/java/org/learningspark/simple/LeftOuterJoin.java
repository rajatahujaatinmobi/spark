package org.learningspark.simple;

import au.com.bytecode.opencsv.CSVReader;
import com.google.common.base.Optional;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;
import java.util.Map;

/**
 * Left Outer Join Example
 */
public class LeftOuterJoin {
  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("Left Outer Join").setMaster("local[*]");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


    // Read the source file
    JavaRDD<String> adInput = sparkContext.textFile("/home/hadoop/sparkscalarepo/learning-spark/learningspark/src/main/resources/ads.csv");

    // Now we have non-empty lines, lets split them into words
    JavaPairRDD<String, String> adsRDD = adInput.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        CSVReader csvReader = new CSVReader(new StringReader(s));
        // lets skip error handling here for simplicity
        try {
          String[] adDetails = csvReader.readNext();
          return new Tuple2<String, String>(adDetails[0], adDetails[1]);
        } catch (IOException e) {
          e.printStackTrace();
          // noop
        }
        // Need to explore more on error handling
        return new Tuple2<String, String>("-1", "1");
      }
    });

    // Read the impressions
    JavaRDD<String> impressionInput = sparkContext.textFile("/home/hadoop/sparkscalarepo/learning-spark/learningspark/src/main/resources/impression.csv");

    // Now we have non-empty lines, lets split them into words
    JavaPairRDD<String, String> impressionsRDD = impressionInput.mapToPair(new PairFunction<String, String, String>() {
      @Override
      public Tuple2<String, String> call(String s) {
        CSVReader csvReader = new CSVReader(new StringReader(s));
        // lets skip error handling here for simplicity
        try {
          String[] adDetails = csvReader.readNext();
          return new Tuple2<String, String>(adDetails[0], adDetails[1]);
        } catch (IOException e) {
          e.printStackTrace();
          // noop
        }
        // Need to explore more on error handling
        return new Tuple2<String, String>("-1", "1");
      }
    });


    // Lets go for an inner join, to hold data only for Ads which received an impression
    JavaPairRDD joinedData = adsRDD.fullOuterJoin(impressionsRDD).fullOuterJoin(adsRDD);
//    joinedData.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<String, Object>>, String, Map<Class, Object>>() {
//      @Override
//      public Iterator<Tuple2<String, Map<Class, Object>>> call(Iterator<Tuple2<String, Object>> tuple2Iterator ) throws Exception {
//        return null;
//      }
//    })
//    joinedData.mapPartitionsToPair(new PairFlatMapFunction() {
//      @Override
//      public Iterable<Tuple2<String, Map<Class, Object>>> call(Iterator<Tuple2<String, Object>> tuple2Iterator) throws Exception {
//        return null;
//      }
//    });
//    joinedData.mapPartitionsToPair(
//            new PairFlatMapFunction<Iterator<Tuple2<String, Object>>, String, Map<Class, Object>>() {
//              @Override
//              public Iterator<Tuple2<String, Map<Class, Object>>> call(Iterator<Tuple2<String, Object>> tuple2Iterator) throws Exception {
//                return null;
//              }
//            }
//    );


    joinedData.saveAsTextFile("/home/hadoop/sparkscalarepo/learning-spark/learningspark/src/main/resources/output-outerjoinagain");

  }
}
