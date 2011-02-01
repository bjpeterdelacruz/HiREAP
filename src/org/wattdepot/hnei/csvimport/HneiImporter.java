package org.wattdepot.hnei.csvimport;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.OverwriteAttemptedException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.RowParseException;
import org.wattdepot.datainput.RowParser;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.hnei.csvimport.validation.MonotonicallyIncreasingValue;
import org.wattdepot.hnei.csvimport.validation.Validator;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;
import au.com.bytecode.opencsv.CSVReader;

/**
 * Reads data from CSV files provided by HNEI (delimited by commas), creates a SensorData object for
 * each line, and sends the SensorData objects to a WattDepot server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiImporter {

  /** Log file for this application. */
  private static final Logger log = Logger.getLogger(HneiImporter.class.getName());

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

  /** Counts number of entries added to server. */
  protected int numEntriesProcessed;

  /** Counts number of entries that are invalid, e.g. do not contain any readings. */
  protected int numInvalidEntries;

  /** Counts total number of entries found in CSV file. */
  protected int numTotalEntries;

  /** Number of entries whose values are not monotonically increasing over time. */
  protected int numNonmonoIncrVals;

  /** Number of daily readings. */
  protected int numDaily;

  /** Number of hourly readings. */
  protected int numHourly;

  /**
   * Creates a new HneiTabularFileSensor object.
   * 
   * @param filename File that contains data for sources.
   * @param uri URI for WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public HneiImporter(String filename, String uri, String username, String password,
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
    this.numInvalidEntries = 0;
    this.numEntriesProcessed = 0;
    this.numTotalEntries = 0;
    this.numNonmonoIncrVals = 0;
    this.numDaily = 0;
    this.numHourly = 0;
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
   * Sets up the logger and file handler.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean setupLogger() {
    log.setLevel(Level.INFO);
    try {
      long timeInMillis = Calendar.getInstance().getTimeInMillis();
      txtFile = new FileHandler("HneiTabularFileSensorLog-" + timeInMillis + ".log");
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
   * Stores a source on a WattDepot server if it does not exist yet and then stores sensor data for
   * that source.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param source Source that is described by the sensor data.
   * @param datum Sensor data for a source.
   * @return True if source and/or sensor data were stored successfully on WattDepot server.
   */
  public boolean process(WattDepotClient client, Source source, SensorData datum) {
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
   * Prints results of parsing CSV file to standard output and log file.
   * 
   * @param importStartTime Start time of import.
   * @param importEndTime End time of import.
   * @param startTimestamp Date of first entry in CSV file.
   * @param endTimestamp Date of last entry in CSV file.
   * @param validateStartTime Start time of validation.
   * @param validateEndTime End time of validation.
   */
  public void printStats(long importStartTime, long importEndTime,
      XMLGregorianCalendar startTimestamp, XMLGregorianCalendar endTimestamp,
      long validateStartTime, long validateEndTime) {
    String msg = "\n\n==================================================\n";
    msg += "Statistics\n";
    msg += "--------------------------------------------------\n";
    msg += "Filename                      : " + this.filename;
    msg += "\n\nFirst Entry Date              : " + startTimestamp.toString() + "\n";
    msg += "Last Entry Date               : " + endTimestamp.toString();
    msg += "\n\nEntries Processed             : " + this.numEntriesProcessed + "\n";
    msg += "Invalid Entries               : " + this.numInvalidEntries + "\n";
    msg += "Percentage of Invalid Entries : ";
    double percentage = ((double) this.numInvalidEntries / (double) this.numTotalEntries) * 100.0;
    msg += String.format("%.2f", percentage);
    msg += "%\n";
    msg += "Total Number of Entries       : " + this.numTotalEntries;
    msg += "\n\nNew Sources                   : " + this.numNewSources + "\n";
    msg += "Existing Sources              : " + this.numExistingSources + "\n";
    msg += "Total Number of Sources       : " + this.numTotalSources;
    msg += "\n\nNumber of Hourly Data         : " + this.numHourly + "\n";
    msg += "Number of Daily Data          : " + this.numDaily;
    msg += "\n\nNew Data                      : " + this.numNewData + "\n";
    msg += "Existing Data                 : " + this.numExistingData + "\n";
    msg += "Total Number of Data Imported : " + (this.numNewData + this.numExistingData);
    msg +=
        "\n\nImport Runtime                : " + this.getRuntime(importStartTime, importEndTime)
            + "\n";
    msg +=
        "Validation Runtime            : " + this.getRuntime(validateStartTime, validateEndTime)
            + "\n";
    msg +=
        "Total Runtime                 : " + this.getRuntime(importStartTime, validateEndTime)
            + "\n\n";
    try {
      long numSourcesPerSecond = this.numTotalSources / ((importEndTime - importStartTime) / 1000);
      msg += "-- " + numSourcesPerSecond + " entries processed per second.\n";
    }
    catch (ArithmeticException e) {
      msg += "-- Number of entries processed per second is immeasurable.\n";
    }
    msg +=
        "-- Number of entries with data that are not monotonically increasing: "
            + this.numNonmonoIncrVals;
    log.log(Level.INFO, msg);
    System.out.print(msg);
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
    HneiImporter inputClient = new HneiImporter(filename, serverUri, username, password, true);
    WattDepotClient client = new WattDepotClient(serverUri, username, password);
    if (client.isHealthy() && client.isAuthenticated()) {
      System.out.println("Successfully connected to " + client.getWattDepotUri() + ".");
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
      System.exit(1);
    }

    if (!inputClient.setupLogger()) {
      System.exit(1);
    }

    List<Entry> entries = new ArrayList<Entry>();
    long importStartTime = 0;
    long importEndTime = 0;
    long validateStartTime = 0;
    long validateEndTime = 0;
    SensorData datum = null;
    XMLGregorianCalendar startTimestamp = null;
    XMLGregorianCalendar endTimestamp = null;

    try {
      boolean isImported = false;
      int counter = 1;
      String source = null;
      String[] line = null;

      System.out.println("Reading in CSV file...\n");

      importStartTime = Calendar.getInstance().getTimeInMillis();
      // for (int i = 0; i < 600; i++) {
      // line = reader.readNext();
      while ((line = reader.readNext()) != null) {
        source = line[0];
        inputClient.setSourceName(source);
        inputClient.setParser();
        try {
          datum = inputClient.parser.parseRow(line);
          if (datum == null) {
            inputClient.numInvalidEntries++;
          }
          else {
            entries.add(new Entry(source, datum.getProperty("reading"), datum.getTimestamp()));
            isImported = inputClient.process(client, new Source(source, username, true), datum);
            if (isImported) {
              inputClient.numEntriesProcessed++;
              if (startTimestamp == null) {
                startTimestamp = datum.getTimestamp();
              }
              endTimestamp = datum.getTimestamp();
            }
            else {
              inputClient.numInvalidEntries++;
            }
          }
        }
        catch (RowParseException e) {
          log.log(Level.SEVERE, "There was a problem parsing the entry for source " + source);
          inputClient.numInvalidEntries++;
        }
        inputClient.numTotalEntries++;
        if ((++counter % 500) == 0) {
          System.out.println("Processing line " + counter + " in " + inputClient.filename + "...");
        }
      }
      importEndTime = Calendar.getInstance().getTimeInMillis();
    }
    catch (IOException e) {
      String msg = "There was a problem reading in the input file:\n" + e.toString();
      msg += "\n\nExiting...";
      System.err.println(msg);
      log.log(Level.SEVERE, msg);
      System.exit(1);
    }

    // Done importing file. Now do some post-processing.
    System.out.print("Checking if readings are monotonically increasing and ");
    System.out.println("are either hourly or daily... This may take a while.");

    int counter = 1;
    List<SensorData> data = null;

    validateStartTime = Calendar.getInstance().getTimeInMillis();
    try {
      Validator monoIncrVal = new MonotonicallyIncreasingValue(client);
      Calendar day = null;
      XMLGregorianCalendar start = null;
      XMLGregorianCalendar end = null;
      String isIncreasing = "isMonotonicallyIncreasing";
      for (Entry e : entries) {
        datum = client.getSensorData(e.getSourceName(), e.getTimestamp());

        // Check if readings are monotonically increasing.
        if (!monoIncrVal.validateEntry(e)) {
          log.log(Level.WARNING, monoIncrVal.getErrorMessage());
          datum.getProperties().getProperty().remove(new Property(isIncreasing, "true"));
          datum.getProperties().getProperty().add(new Property(isIncreasing, "false"));
          inputClient.numNonmonoIncrVals++;
        }

        // Classify data as either hourly or daily.
        day = Calendar.getInstance();
        day.set(e.getTimestamp().getYear(), e.getTimestamp().getMonth() - 1, e.getTimestamp()
            .getDay(), 0, 0, 0);
        start = Tstamp.makeTimestamp(day.getTime().getTime());
        end = Tstamp.incrementSeconds(Tstamp.incrementDays(start, 1), -1);

        data = client.getSensorDatas(e.getSourceName(), start, end);

        if (data.size() == 1) {
          datum.getProperties().getProperty().remove(new Property("hourly", "true"));
          inputClient.numDaily++;
        }
        else {
          datum.getProperties().getProperty().remove(new Property("daily", Boolean.toString(true)));
          inputClient.numHourly++;
        }

        // Update sensor data.
        client.deleteSensorData(e.getSourceName(), e.getTimestamp());
        client.storeSensorData(datum);

        if (++counter % 500 == 0) {
          System.out.println("Processing entry " + counter + "...");
        }
      }
      validateEndTime = Calendar.getInstance().getTimeInMillis();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      System.exit(1);
    }
    catch (JAXBException e) {
      e.printStackTrace();
      System.exit(1);
    }

    inputClient.printStats(importStartTime, importEndTime, startTimestamp, endTimestamp,
        validateStartTime, validateEndTime);

  }

}
