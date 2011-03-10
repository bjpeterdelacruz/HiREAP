package org.wattdepot.hnei.csvimport;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This class uses reflection to call the appropriate processCsvFile method to process a CSV file
 * containing energy data.
 * 
 * @author BJ Peter DeLaCruz
 */
public class CsvImporter {

  /**
   * Command-line program that reads in the first two rows of a CSV file to determine which method
   * to call to process the CSV file; uses reflection to call the appropriate method.
   * 
   * @param args Contains filename, server URI, username, and password.
   */
  public static void main(String[] args) {
    if (args.length < 4 || args.length > 5) {
      System.err.print("Expected at least 4 command-line arguments: [filename] [server_uri] ");
      System.err.println("[username] [password] optional: [-e|-hnei|-hobo]");
      System.err.println("Please try again.");
      System.exit(1);
    }

    // Open CSV file for reading.
    CSVReader reader = null;
    try {
      reader = new CSVReader(new FileReader(args[0]), ',', CSVReader.DEFAULT_QUOTE_CHARACTER, 0);
    }
    catch (FileNotFoundException e) {
      System.err.println("File not found! Exiting...");
      System.exit(1);
    }

    // Get first two rows in CSV file.
    String[] header = null;
    String[] firstRow = null;
    try {
      header = reader.readNext();
      firstRow = reader.readNext();
      reader.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Ask user if first row should be skipped when processing CSV file.
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String command = "";
    boolean skipFirstRow = true;

    try {
      while (!"y".equalsIgnoreCase(command) && !"n".equalsIgnoreCase(command)) {
        System.out.print("Skip first row in " + args[0] + " [y|n]? ");
        if ((command = br.readLine()) == null) {
          System.out.println("Error encountered while trying to read in number of sources.");
          System.exit(1);
        }
        if (!"y".equalsIgnoreCase(command) && !"n".equalsIgnoreCase(command)) {
          System.out.println("Please enter \"y\" to skip the first row or \"n\" to process it.");
        }
      }
      if ("n".equalsIgnoreCase(command)) {
        skipFirstRow = false;
      }
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Determine which class to use given first or second row.
    String packageName = "org.wattdepot.hnei.csvimport.";
    String className = null;
    String option = "";
    if (args.length == 5) {
      option = args[4];
    }
    if ("Account".equalsIgnoreCase(header[0]) && "Install Date".equalsIgnoreCase(header[1])
        || "-hnei".equalsIgnoreCase(option)) {
      className = "HneiImporter";
    }
    // else if ("Whole House".equalsIgnoreCase(header[1]) && "AC".equalsIgnoreCase(header[2])
       // || "-e".equalsIgnoreCase(option)) {
       // className = "EgaugeImporter";
    // }
    else if ("RH, %".equalsIgnoreCase(firstRow[3]) || "-hobo".equalsIgnoreCase(option)) {
      className = "HoboImporter";
    }
    else {
      System.err.println("Illegal argument specified [" + option + "].");
      System.exit(1);
    }

    // Call processCsvFile method in appropriate class.
    try {
      Class<?> cls = Class.forName(packageName + className);
      Constructor<?> constructor =
          cls.getConstructor(String.class, String.class, String.class, String.class, Boolean.TYPE);
      Object obj = constructor.newInstance(args[0], args[1], args[2], args[3], skipFirstRow);
      Method method = cls.getDeclaredMethod("processCsvFile", (Class<?>[]) null);
      Boolean isSuccessful = (Boolean) method.invoke(obj, (Object[]) null);
      if (!isSuccessful.booleanValue()) {
        System.err.println("The method failed to terminate successfully.");
        System.exit(1);
      }
    }
    catch (ClassNotFoundException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (IllegalAccessException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (IllegalArgumentException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (SecurityException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (InvocationTargetException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (NoSuchMethodException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (InstantiationException e) {
      e.printStackTrace();
      System.exit(1);
    }

    System.out.println("Import successful!");
  }

}
