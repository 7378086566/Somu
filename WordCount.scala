package com.practise.spark

import org.apache.spark.sql.SparkSession

object WordCount {

  def main(args: Array[String]) {

    // Create SparkSession to instantiate the application    
    val spark = SparkSession.builder().appName("WordCount").master("local[*]").getOrCreate();
    // Create an RDD from data source of given directory having all csv files  
    val lines = spark.sparkContext.textFile("../SparkScalaCourse/src/com/practise/spark/*.csv", 4)
    // count the number of lines
    val countNo = lines.count()
    // print total on the console
    println("Total No Of Rows: " + countNo)
    //flatten each word of the line of each file with split
    val words = lines.flatMap(x => x.split(","));
    // Count of the occurrences of each word
    val wordCounts = words.map(x => (x, 1)).reduceByKey((x, y) => x + y)
    // Flip (word, count) tuples to (count, word) and then sort by key (the counts)
    val wordCountsSorted = wordCounts.map(x => (x._1, x._2)).sortBy(_._2, false)
    //repartition the output to single file only 
    wordCountsSorted.repartition(1).saveAsTextFile("../SparkScalaCourse/src/com/practise/spark/output.csv")
  }

}
