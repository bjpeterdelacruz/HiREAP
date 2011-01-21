package org.wattdepot.hnei;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.xml.bind.JAXBException;
import org.wattdepot.client.OverwriteAttemptedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.RowParseException;
import org.wattdepot.datainput.RowParser;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import au.com.bytecode.opencsv.CSVReader;

/**
 * Reads data from CSV files provided by HNEI (delimited by commas), creates a SensorData object for
 * each line, object for each line, and sends the SensorData objects to a WattDepot server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiTabularFileSensor {

  /** Log file for this application. */
  private static final Logger log = Logger.getLogger(HneiTabularFileSensor.class.getName());

  /** Output logging information to a text file. */
  private static FileHandler txtFile;

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
  protected static final String toolName = "HneiTabularFileSensor";

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

  /**
   * Creates a new HneiTabularFileSensor object.
   * 
   * @param filename File that contains data for sources.
   * @param uri URI for WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public HneiTabularFileSensor(String filename, String uri, String username, String password,
      boolean skipFirstRow) {
    this.filename = filename;
    this.serverUri = uri;
    this.sourceName = null;
    this.username = username;
    this.password = password;
    this.skipFirstRow = skipFirstRow;
    this.parser = new HneiCsvRowParser(toolName, this.serverUri, null, log);
    this.numNewSources = 0;
    this.numExistingSources = 0;
    this.numTotalSources = 0;
    this.numNewData = 0;
    this.numExistingData = 0;
  }

  /**
   * Sets the source name.
   * 
   * @param sourceName Name of a source.
   */
  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  /**
   * Sets the parser. Called after setting source name.
   */
  public void setParser() {
    this.parser = new HneiCsvRowParser(toolName, this.serverUri, this.sourceName, log);
  }

  /**
   * Sets up the logger and file handler.
   */
  public static void setupLogger() {
    log.setLevel(Level.INFO);
    try {
      long timeInMillis = Calendar.getInstance().getTimeInMillis();
      txtFile = new FileHandler("HneiTabularFileSensorLog-" + timeInMillis + ".txt");
      txtFile.setFormatter(new SimpleFormatter());
    }
    catch (IOException e) {
      System.err.println("Unable to create file handler for logger.");
      System.exit(1);
    }
    log.addHandler(txtFile);
  }

  /**
   * Stores a source on a WattDepot server if it does not exist yet and then stores sensor data for
   * that source.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param source Source that is described by the sensor data.
   * @param datum Sensor data for a source.
   * @return true if source and/or sensor data were stored successfully on WattDepot server.
   */
  public boolean process(WattDepotClient client, Source source, SensorData datum) {
    if (datum == null) {
      log.log(Level.INFO, "No data for source " + source.getName() + ".\n");
      return false;
    }

    try {
      try {
        client.storeSource(source, false);
        this.numNewSources++;
      }
      catch (OverwriteAttemptedException e) {
        this.numExistingSources++;
        log.log(Level.INFO, "Source " + source.getName() + " already exists on server.\n");
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
   * Given a CSV file with lots of sources, this program will parse each row, which represents a
   * source; create a SensorData object from each row, and store the sensor data for the source on a
   * WattDepot server.
   * 
   * @param args Contains filename, server URI, username, and password.
   */
  public static void main(String[] args) {
    if (args.length != 4) {
      System.err.println("Command-line arguments not in correct format. Exiting...");
      System.exit(1);
    }

    String filename = args[0];
    String serverUri = args[1];
    String username = args[2];
    String password = args[3];

    // Open CSV file for reading.
    CSVReader reader = null;
    try {
      reader = new CSVReader(new FileReader(filename), ',', CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
    }
    catch (FileNotFoundException e) {
      System.err.println("File not found! Exiting...");
      System.exit(1);
    }

    // Grab data from CSV file.
    HneiTabularFileSensor inputClient =
        new HneiTabularFileSensor(filename, serverUri, username, password, true);
    WattDepotClient client = new WattDepotClient(serverUri, username, password);

    setupLogger();

    String source = null;
    String[] line = null;
    try {
      SensorData datum = null;

      System.out.println("Reading in CSV file...\n");
      while ((line = reader.readNext()) != null) {
        source = line[0];
        inputClient.setSourceName(source);
        inputClient.setParser();
        try {
          datum = inputClient.parser.parseRow(line);
          inputClient.process(client, new Source(source, username, true), datum);
        }
        catch (RowParseException e) {
          log.log(Level.SEVERE, "There was a problem parsing the entry for source " + source);
        }
      }
    }
    catch (IOException e) {
      String msg = "There was a problem reading in the input file:\n" + e.toString();
      msg += "\n\nExiting...";
      System.err.println(msg);
      log.log(Level.SEVERE, msg);
      System.exit(1);
    }

    // Print list of sources.
    /*
     * try { sources = client.getSources(); for (Source src : sources) {
     * System.out.println(src.toString()); } } catch (Exception e) {
     * System.err.println(e.toString()); }
     */

    System.out.println("Statistics:\n");
    System.out.println("New Sources: " + inputClient.numNewSources);
    System.out.println("Existing Sources: " + inputClient.numExistingSources);
    System.out.println("Total Sources: " + inputClient.numTotalSources);
    System.out.println("New Data: " + inputClient.numNewData);
    System.out.println("Existing Data: " + inputClient.numExistingData + "\n");
  }

}
