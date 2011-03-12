package org.wattdepot.hnei.csvimport;

import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

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
    if (args.length < 3) {
      System.err.print("Expected at least 4 command-line arguments: [-s server_uri] [-u username]");
      System.err.println("[-p password] [-d (egauge | hnei | hobo)] [-x] [-m]");
      System.err.println("Please try again.");
      System.exit(1);
    }

    Options options = new Options();
    options.addOption("s", true, "Server URI.");
    options.addOption("u", true, "Username.");
    options.addOption("p", true, "Password.");
    options.addOption("d", true, "Type of data file(s) to import.");
    options.addOption("x", false, "If specified, skip first line in all data files.");
    String msg = "If specified, ask user if next file in current directory should be imported;";
    msg += " otherwise, import all files automatically.";
    options.addOption("m", false, msg);

    BasicParser parser = new BasicParser();
    CommandLine cl = null;
    boolean autoMode = true;
    boolean skipFirstRow = false;
    try {
      cl = parser.parse(options, args);
      if (!cl.hasOption("s") || !cl.hasOption("u") || !cl.hasOption("p") || !cl.hasOption("d")) {
        msg = "At least one of the required options was not specified.";
        System.err.println(msg);
        System.exit(1);
      }
      if (cl.hasOption("x")) {
        skipFirstRow = true;
      }
      if (cl.hasOption("m")) {
        autoMode = false;
      }
    }
    catch (org.apache.commons.cli.UnrecognizedOptionException e) {
      System.err.println(e.getMessage() + ". Please try again.");
      System.exit(1);
    }
    catch (org.apache.commons.cli.ParseException e) {
      e.printStackTrace();
      System.exit(1);
    }

    // Determine which class to use given argument after -d flag.
    String csvFileType = null;
    String packageName = "org.wattdepot.hnei.csvimport";

    if (cl.getOptionValue("d").equals("hnei")) {
      csvFileType = "Hnei";
    }
    else if (cl.getOptionValue("d").equals("egauge")) {
      csvFileType = "Egauge";
    }
    else if (cl.getOptionValue("d").equals("hobo")) {
      csvFileType = "Hobo";
    }

    if (csvFileType == null) {
      System.err.println("Header row not in correct format.");
      System.exit(1);
    }

    // Use reflection to find class that has appropriate processCsvFile method to call.
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
        if (a.toString().contains(csvFileType)) {
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
      String response = null;
      String[] children = Importer.getAllCsvFiles();
      boolean processNextFile = true;

      Constructor<?> constructor =
          cls.getConstructor(String.class, String.class, String.class, String.class, Boolean.TYPE);

      Object obj = null;
      Method processCsvFile = null;
      Method closeLogger = null;
      Boolean isSuccessful = null;

      String file = null;
      for (int index = 0; index < children.length; index++) {
        if (processNextFile) {
          file = children[index];
          obj =
              constructor.newInstance(file, cl.getOptionValue("s"), cl.getOptionValue("u"),
                  cl.getOptionValue("p"), skipFirstRow);
          processCsvFile = cls.getDeclaredMethod("processCsvFile", (Class<?>[]) null);
          isSuccessful = (Boolean) processCsvFile.invoke(obj, (Object[]) null);
          if (!isSuccessful.booleanValue()) {
            System.err.println("The method failed to terminate successfully.");
            System.exit(1);
          }
          closeLogger = cls.getMethod("closeLogger", (Class<?>[]) null);
          isSuccessful = (Boolean) closeLogger.invoke(obj, (Object[]) null);
          if (!isSuccessful.booleanValue()) {
            System.err.println("The method failed to terminate successfully.");
            System.exit(1);
          }
        }

        if (!autoMode) {
          if (index == children.length - 1) {
            break;
          }

          response = Importer.processNextFile(children[index + 1]);
          if ("no".equalsIgnoreCase(response)) {
            processNextFile = false;
          }
          else if ("quit".equalsIgnoreCase(response)) {
            break;
          }
          else {
            processNextFile = true;
          }
        }
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
