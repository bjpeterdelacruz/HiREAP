package org.wattdepot.hnei.csvimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This class uses reflection to call the appropriate processCsvFile method to process a CSV file
 * containing energy data.
 * 
 * @author BJ Peter DeLaCruz
 */
public class CsvImporter {

  /**
   * Recursive method used to find all classes in a given directory and sub-directories.
   * 
   * @param directory The base directory to search in.
   * @param packageName The package to which the classes to search for belong.
   * @return List of classes in given directory and sub-directories.
   */
  private List<Class<?>> findClasses(String directory, String packageName) {
    List<Class<?>> classes = new ArrayList<Class<?>>();
    File f = new File(directory);
    if (!f.isDirectory()) {
      return null;
    }
    File[] files = f.listFiles();
    for (File file : files) {
      if (file.isDirectory()) {
        classes.addAll(findClasses(directory + "/" + file.getName(), packageName));
      }
      else if (file.getName().endsWith(".class")) {
        String className = packageName + "." + file.getName().replace(".class", "");
        try {
          classes.add(Class.forName(className));
        }
        catch (ClassNotFoundException e) {
          continue;
        }
      }
    }
    return classes;
  }

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
    String packageName = "org.wattdepot.hnei.csvimport";
    String className = null;
    String option = "";
    if (args.length == 5) {
      option = args[4];
    }

    boolean isValidFile = false;
    if ("Account".equalsIgnoreCase(header[0]) && "Install Date".equalsIgnoreCase(header[1])
        || "-hnei".equalsIgnoreCase(option)) {
      className = "Hnei";
      isValidFile = true;
    }
    else if ("Whole House".equalsIgnoreCase(header[1]) && "AC".equalsIgnoreCase(header[2])
        || "-e".equalsIgnoreCase(option)) {
      className = "Egauge";
      isValidFile = true;
    }
    else if ("RH, %".equalsIgnoreCase(firstRow[3]) || "-hobo".equalsIgnoreCase(option)) {
      className = "Hobo";
      isValidFile = true;
    }

    if (!"-hnei".equalsIgnoreCase(option) && !"-e".equalsIgnoreCase(option)
        && !"-hobo".equalsIgnoreCase(option)) {
      System.err.println("Illegal argument specified [" + option + "].");
      System.exit(1);
    }

    if (!isValidFile) {
      System.err.println("Header row not in correct format.");
      System.exit(1);
    }

    // Use reflection to find the class that has the appropriate processCsvFile method to call.
    Annotation[] annotations = null;
    Class<?> cls = null;
    CsvImporter importer = new CsvImporter();
    List<Class<?>> classes = importer.findClasses(System.getProperty("user.dir"), packageName);
    if (classes == null) {
      System.err.println("Directory not specified.");
      System.exit(1);
    }

    for (Class<?> c : classes) {
      annotations = c.getAnnotations();
      for (Annotation a : annotations) {
        if (a.toString().contains(className)) {
          cls = c;
        }
      }
    }
    if (cls == null) {
      System.err.println("Unable to find Java class to process CSV file.");
      System.exit(1);
    }

    // Call processCsvFile method in appropriate class.
    try {
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
