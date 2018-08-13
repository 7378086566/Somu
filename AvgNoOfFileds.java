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
