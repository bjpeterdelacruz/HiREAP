package org.wattdepot.hnei.csvimport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Calendar;
import java.util.List;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.xml.bind.JAXBException;
import org.wattdepot.client.OverwriteAttemptedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.RowParser;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;

/**
 * Classes that implement this abstract class will import all kinds of data from CSV files.
 * 
 * @author BJ Peter DeLaCruz
 */
public abstract class Importer {

  /** Log file for this application. */
  protected Logger log;

  /** Output logging information to a text file. */
  protected FileHandler txtFile;

  /** Name of the file to be input. */
  protected String filename;

  /** URI of WattDepot server to send data to. */
  protected String serverUri;

  /** Username to use when sending data to server. */
  protected String username;

  /** Password to use when sending data to server. */
  protected String password;

  /** Whether or not to skip the first row of the file. */
  protected boolean skipFirstRow;

  /** Name of the application on the command line. */
  protected String toolName;

  /** The parser used to turn rows into SensorData objects. */
  protected RowParser parser;

  /** Counts number of new sources added to the WattDepot server. */
  protected int numNewSources;

  /** Counts number of sources that are already on the WattDepot server. */
  protected int numExistingSources;

  /** Counts all sources that are in the CSV file. */
  protected int numTotalSources;

  /** Counts number of new data imported. */
  protected int numNewData;

  /** Counts number of data that already exists on the WattDepot server. */
  protected int numExistingData;

  /** Counts number of entries added to server. */
  protected int numEntriesProcessed;

  /** Counts number of entries that are invalid, e.g. do not contain any readings. */
  protected int numInvalidEntries;

  /** Counts total number of entries found in CSV file. */
  protected int numTotalEntries;

  /** Number of daily readings. */
  protected int numDaily;

  /** Number of hourly readings. */
  protected int numHourly;

  /** List of all entries in CSV file. */
  protected List<Entry> entries;

  /** List of all sources and their MTU IDs, with duplicates. */
  protected List<Entry> allSources;

  /** List of all sources and their MTU IDs, no duplicates. */
  protected Set<Entry> allMtus;

  /** List of all sources that have multiple MTU IDs. */
  protected List<Entry> allDuplicateMtus;

  /** List of all entries that have values that not monotonically increasing. */
  protected List<Entry> allNonmonoIncrVals;

  /** Start time in seconds to import data from CSV file. */
  protected long importStartTime;

  /** End time in seconds to import data from CSV file. */
  protected long importEndTime;

  /**
   * Returns the parser used to get rows from CSV files.
   * 
   * @return Parser used to get rows from CSV files.
   */
  public RowParser getParser() {
    return this.parser;
  }

  /**
   * Sets up the logger and file handler.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean setupLogger() {
    this.log.setLevel(Level.INFO);
    try {
      long timeInMillis = Calendar.getInstance().getTimeInMillis();
      this.txtFile = new FileHandler(this.toolName + "-" + timeInMillis + ".log");
      this.txtFile.setFormatter(new SimpleFormatter());
    }
    catch (IOException e) {
      System.err.println("Unable to create file handler for logger.");
      return false;
    }
    catch (SecurityException e) {
      return false;
    }
    this.log.addHandler(this.txtFile);
    return true;
  }

  /**
   * Closes the log file.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean closeLogger() {
    try {
      this.txtFile.close();
      return true;
    }
    catch (SecurityException e) {
      return false;
    }
  }

  /**
   * Stores a source on the WattDepot server if it is not already on there.
   * 
   * @param client Used to store a source on the WattDepot server.
   * @param sourceName Name of a source.
   * @return True if successful, false otherwise.
   */
  public boolean storeSource(WattDepotClient client, String sourceName) {
    try {
      Source source = new Source(sourceName, this.username, true);
      source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
      client.storeSource(source, false);
    }
    catch (OverwriteAttemptedException e) {
      String msg = "Source " + sourceName + " already exists on server.\n";
      System.out.print(msg);
      log.log(Level.INFO, msg);
    }
    catch (WattDepotClientException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    catch (JAXBException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    return true;
  }

  /**
   * Stores a source on a WattDepot server if it does not exist yet and then stores sensor data for
   * that source.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param source Source that is described by the sensor data.
   * @param data Sensor data for a source.
   * @return True if source and/or sensor data were stored successfully on WattDepot server, false
   * otherwise.
   */
  public boolean process(WattDepotClient client, Source source, SensorData data) {
    try {
      try {
        source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
        client.storeSource(source, false);
        this.numNewSources++;
      }
      catch (OverwriteAttemptedException e) {
        this.numExistingSources++;
        // log.log(Level.INFO, "Source " + source.getName() + " already exists on server.\n");
      }
      this.numTotalSources++;
      client.storeSensorData(data);
      this.numNewData++;
    }
    catch (OverwriteAttemptedException e) {
      this.numExistingData++;
      String msg = "Data at " + data.getTimestamp().toString() + " for " + source.getName();
      msg += " already exists on server.\n";
      log.log(Level.INFO, msg);
    }
    catch (WattDepotClientException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    catch (JAXBException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    return true;
  }

  /**
   * Stores a source on a WattDepot server if it does not exist yet.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param source Source that is described by the sensor data.
   * @return True if successful, false otherwise.
   */
  public boolean process(WattDepotClient client, Source source) {
    try {
      try {
        source.addProperty(new Property(Source.SUPPORTS_ENERGY_COUNTERS, "true"));
        client.storeSource(source, false);
        this.numNewSources++;
      }
      catch (OverwriteAttemptedException e) {
        this.numExistingSources++;
        // log.log(Level.INFO, "Source " + source.getName() + " already exists on server.\n");
      }
      this.numTotalSources++;
    }
    catch (WattDepotClientException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    catch (JAXBException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    return true;
  }

  /**
   * Stores sensor data for a source that already exists on WattDepot server.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param data Sensor data for a source.
   * @return True if successful, false otherwise.
   */
  public boolean process(WattDepotClient client, SensorData data) {
    try {
      client.storeSensorData(data);
      this.numNewData++;
    }
    catch (OverwriteAttemptedException e) {
      this.numExistingData++;
      String msg = "Data at " + data.getTimestamp().toString() + " already exists on server.\n";
      log.log(Level.INFO, msg);
    }
    catch (WattDepotClientException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    catch (JAXBException e) {
      System.err.println(e.toString());
      log.log(Level.SEVERE, e.toString());
      return false;
    }
    return true;
  }

  /**
   * Converts the runtime in milliseconds to the string format hh:mm:ss.
   * 
   * @param startTime Start time of a run.
   * @param endTime End time of a run.
   * @return Time in string format hh:mm:ss.
   */
  public static String getRuntime(long startTime, long endTime) {
    long milliseconds = endTime - startTime;
    long hours = milliseconds / (1000 * 60 * 60);
    long minutes = (milliseconds % (1000 * 60 * 60)) / (1000 * 60);
    long seconds = ((milliseconds % (1000 * 60 * 60)) % (1000 * 60)) / 1000;
    return String.format("%02d:%02d:%02d", hours, minutes, seconds);
  }

  /**
   * Returns a list of CSV files to process.
   * 
   * @param dirName Name of the directory where the CSV files are located.
   * @return List of CSV files in current working directory.
   */
  public static String[] getAllCsvFiles(String dirName) {
    File dir = new File(dirName);

    FilenameFilter filter = new FilenameFilter() {
      public boolean accept(File dir, String name) {
        return name.startsWith("012658") && name.endsWith("csv");
      }
    };
    return dir.list(filter);
  }

  /**
   * Asks the user if he or she wants to process or skip next file, or quit program.
   * 
   * @param fname Name of next file to process.
   * @return "yes" if user wants to process next file, "no" if user wants to skip next file, or
   * "quit" to exit program.
   */
  public static String processNextFile(String fname) {
    BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
    String command = "";
    String msg = "Do you want to process the next file [" + fname + "]";
    msg += "or quit the program [ yes | no | quit ]? ";

    System.out.print(msg);

    try {
      if ((command = br.readLine()) == null) {
        throw new IOException();
      }
      while (!"yes".equalsIgnoreCase(command) && !"no".equalsIgnoreCase(command)
          && !"quit".equalsIgnoreCase(command)) {
        System.out.println("Please enter \"yes\" or \"no\", or \"quit\" to quit the program.");
        System.out.print(msg);
        if ((command = br.readLine()) == null) {
          throw new IOException();
        }
      }
      if ("yes".equalsIgnoreCase(command)) {
        return "yes";
      }
      else if ("no".equalsIgnoreCase(command)) {
        return "no";
      }
      else {
        return "quit";
      }
    }
    catch (IOException e) {
      System.err.println("There was a problem reading in a command from the keyboard.");
      return "quit";
    }
  }

  /**
   * Prints results of parsing CSV file to standard output and log file.
   */
  public abstract void printStats();

  /**
   * Parses each row, creates a SensorData object from each, and stores the sensor data on a
   * WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  public abstract boolean processCsvFile();

}
