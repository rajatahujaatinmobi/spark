package org.learningspark.simple;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * File Copy program
 */
public class FileCopy {

  public static void main(String[] args) {
    SparkConf sparkConf = new SparkConf().setAppName("File Copy");
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);


    // Read the source file
    JavaRDD<String> input = sparkContext.textFile(args[0]);

    // Save the file to specified location
    input.saveAsTextFile(args[1]);
  }

}
