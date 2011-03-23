package org.wattdepot.hnei.csvimport;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This program takes two CSV files, one with Egauge energy data and another with Egauge power data,
 * and merges the two together.
 * 
 * @author BJ Peter DeLaCruz
 */
public class CsvFileMerger {

  /**
   * Opens a CSV file for reading.
   * 
   * @param filename Name of the CSV file.
   * @param skipFirstRow True to skip first row in CSV file, false otherwise.
   * @return File to read from if successful, null otherwise.
   */
  public CSVReader openFile(String filename, boolean skipFirstRow) {
    try {
      int line = 1;
      if (!skipFirstRow) {
        line = 0;
      }
      char defaultChar = CSVReader.DEFAULT_QUOTE_CHARACTER;
      return new CSVReader(new FileReader(filename), ',', defaultChar, line);
    }
    catch (FileNotFoundException e) {
      System.err.println("File [" + filename + "] not found! Exiting...");
      return null;
    }
  }

  /**
   * Given two CSV files, one with energy data and another with power data, this program will merge
   * the two together.
   * 
   * @param args Two filenames.
   */
  public static void main(String[] args) {
    if (args.length != 2) {
      System.err.println("Command-line arguments not in correct format. Exiting...");
      System.exit(1);
    }

    String filename1 = args[0];
    String filename2 = args[1];
    String outputFilename = args[0].replaceAll(".csv", "-") + args[1];

    CsvFileMerger merger = new CsvFileMerger();
    CSVReader file1 = merger.openFile(filename1, false); // energy data
    if (file1 == null) {
      System.exit(1);
    }
    CSVReader file2 = merger.openFile(filename2, false); // average power data
    if (file2 == null) {
      System.exit(1);
    }

    File outputFile = new File(outputFilename);
    outputFile.setWritable(true);
    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile));
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    int lineCount = 0;
    String line = "";
    String[] line1 = null;
    String[] line2 = null;
    StringBuffer buffer = new StringBuffer();
    List<String> combinedLine = new ArrayList<String>();

    try {
      while ((line1 = file1.readNext()) != null && (line2 = file2.readNext()) != null) {
        // Make energy data come first before power data.
        if (line1[1].contains("[kW]")) {
          CSVReader temp = file1;
          file1 = file2;
          file2 = temp;
          String[] tempLine = line1;
          line1 = line2;
          line2 = tempLine;
        }

        // Do not add lines whose timestamps do not match to the output file.
        // Input files are assumed to be sorted in descending order by timestamp.
        while (line1 != null && !line1[0].equals(line2[0])) {
          line1 = file1.readNext();
        }
        if (line1 == null) {
          break;
        }

        // Add energy data to the array list first and then add power data.
        for (String s : line1) {
          combinedLine.add(s);
        }
        for (int index = 1; index < line2.length; index++) {
          combinedLine.add(line2[index]);
        }

        // Make one long string and write that string to the output file.
        for (String s : combinedLine) {
          s = s + ",";
          buffer.append(s);
        }
        line = buffer.toString().substring(0, buffer.toString().length() - 1) + "\n";

        writer.write(line);

        if (++lineCount % 100 == 0) {
          System.out.println("Processing line number " + lineCount + "...");
        }

        buffer = new StringBuffer();
        combinedLine.clear();
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }
    finally {
      try {
        writer.close();
      }
      catch (IOException e) {
        e.printStackTrace();
        System.exit(1);
      }
    }

    System.out.println("Done processing " + filename1 + " and " + filename2 + "...");
  }

}
