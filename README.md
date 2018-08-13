# Somu
Somu's Github
//1 # what's the average number of fields across all the .csv files?
package com.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class AvgNoOfFileds {
	public static void main(String[] args) throws IOException {
		String directoryPath = args[0];
		// "C:\\SparkScala\\Java\\src\\com\\files\\";
		// File Directory path is an array of list files
		File[] filesInDirectory = new File(directoryPath).listFiles();
		int fileCount = 0;
		int numOfColumns = 0;
		// iterate each file from the Directory
		for (File f : filesInDirectory) {
			// read apsolutePath of each file
			String filePath = f.getAbsolutePath();
			// get the file extension
			String fileExtenstion = filePath.substring(filePath.lastIndexOf(".") + 1, filePath.length());
			// check file extension is csv or not
			if ("csv".equals(fileExtenstion)) {
				fileCount++;
				// System.out.println("file:"+f.getName());
				// read input stream
				BufferedReader br = new BufferedReader(new FileReader(directoryPath + "\\" + f.getName()));
				String line;
				// read the line of the file, split the line by delimiter and sum up the length of it and break it.
				while ((line = br.readLine()) != null) {
					// use comma as separator
					numOfColumns += line.split(",").length;
					// System.out.println("numofColumns:" + numOfColumns);
					break;
				}
			}

		}
		// System.out.println("number of CSV files:" + fileCount);
		// System.out.println("total number of columns in all CSV files:" +
		// numOfColumns);
		// System.out.println("Average number of fields in all CSV files:" +
		// (numOfColumns / fileCount));
		System.out.println((numOfColumns / fileCount));
	}
}

//2#create a csv file that shows the word count of every value of every dataset (dataset being a .csv file)
package com.practise.spark

import org.apache.spark.sql.SparkSession
object RowsCount {
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
//3# what's the total number or rows for the all the .csv files?

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
