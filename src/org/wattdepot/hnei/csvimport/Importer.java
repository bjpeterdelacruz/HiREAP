package org.wattdepot.hnei.csvimport;

import java.io.IOException;
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

  /** Name of Source to send data to. */
  protected String sourceName;

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

  /** Start time in seconds to validate data. */
  protected long validateStartTime;

  /** End time in seconds to validate data. */
  protected long validateEndTime;

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
    log.setLevel(Level.INFO);
    try {
      long timeInMillis = Calendar.getInstance().getTimeInMillis();
      txtFile = new FileHandler(toolName + "-" + timeInMillis + ".log");
      txtFile.setFormatter(new SimpleFormatter());
    }
    catch (IOException e) {
      System.err.println("Unable to create file handler for logger.");
      return false;
    }
    log.addHandler(txtFile);
    return true;
  }

  /**
   * Stores a source on the WattDepot server if it is not already on there.
   * 
   * @param client Used to store a source on the WattDepot server.
   * @return True if successful, false otherwise.
   */
  public boolean storeSource(WattDepotClient client) {
    this.sourceName = this.filename.substring(0, this.filename.lastIndexOf('.'));

    // Store source on WattDepot server.
    try {
      client.storeSource(new Source(sourceName, username, true), false);
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
   * @param datum Sensor data for a source.
   * @return True if source and/or sensor data were stored successfully on WattDepot server, false
   * otherwise.
   */
  public boolean process(WattDepotClient client, Source source, SensorData datum) {
    try {
      try {
        client.storeSource(source, false);
        this.numNewSources++;
      }
      catch (OverwriteAttemptedException e) {
        this.numExistingSources++;
        // log.log(Level.INFO, "Source " + source.getName() + " already exists on server.\n");
      }
      this.numTotalSources++;
      client.storeSensorData(datum);
      this.numNewData++;
    }
    catch (OverwriteAttemptedException e) {
      this.numExistingData++;
      String msg = "Data at " + datum.getTimestamp().toString() + " for " + source.getName();
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
   * Stores a source on a WattDepot server if it does not exist yet and then stores sensor data for
   * that source.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param source Source that is described by the sensor data.
   * @return True if successful, false otherwise.
   */
  public boolean process(WattDepotClient client, Source source) {
    try {
      try {
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
   * @param datum Sensor data for a source.
   * @return True if successful, false otherwise.
   */
  public boolean process(WattDepotClient client, SensorData datum) {
    try {
      client.storeSensorData(datum);
      this.numNewData++;
    }
    catch (OverwriteAttemptedException e) {
      this.numExistingData++;
      String msg = "Data at " + datum.getTimestamp().toString() + " already exists on server.\n";
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
  public String getRuntime(long startTime, long endTime) {
    long milliseconds = endTime - startTime;
    long hours = milliseconds / (1000 * 60 * 60);
    long minutes = (milliseconds % (1000 * 60 * 60)) / (1000 * 60);
    long seconds = ((milliseconds % (1000 * 60 * 60)) % (1000 * 60)) / 1000;
    return String.format("%02d:%02d:%02d", hours, minutes, seconds);
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