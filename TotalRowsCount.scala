package com.practise.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object TotalRowsCount {
  def main(args: Array[String]) {
    // Create SparkSession to instantiate the application    
    val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate();
    // Create an RDD from data source of given directory having all csv files  
    val lines = spark.sparkContext.textFile("../SparkScalaCourse/src/com/practise/spark/*.csv", 4)
    // count the number of lines
    val countNo = lines.count()
    // print total on the console
    println("Total No Of Rows: " + countNo)
  }
}