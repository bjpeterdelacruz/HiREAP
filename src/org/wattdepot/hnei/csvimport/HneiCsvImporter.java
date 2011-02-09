package org.wattdepot.hnei.csvimport;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
import org.wattdepot.hnei.csvimport.validation.EntrySortByTimestamp;
import org.wattdepot.hnei.csvimport.validation.MonotonicallyIncreasingValue;
import org.wattdepot.hnei.csvimport.validation.Validator;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This class reads data from CSV files provided by HNEI (delimited by commas), creates a SensorData
 * object from each line, and stores the SensorData objects to a WattDepot server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvImporter {

  /** Log file for this application. */
  private static final Logger log = Logger.getLogger(HneiCsvImporter.class.getName());

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
  private static final String toolName = "HneiCsvImporter";

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

  /**
   * Creates a new HneiTabularFileSensor object.
   * 
   * @param filename File that contains data for sources.
   * @param uri URI for WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public HneiCsvImporter(String filename, String uri, String username, String password,
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
    this.numDaily = 0;
    this.numHourly = 0;
    this.entries = new ArrayList<Entry>();
    this.allSources = new ArrayList<Entry>();
    this.allMtus = new HashSet<Entry>();
    this.allDuplicateMtus = new ArrayList<Entry>();
    this.allNonmonoIncrVals = new ArrayList<Entry>();
  }

  /**
   * Returns the parser used to get rows from CSV files.
   * 
   * @return Parser used to get rows from CSV files.
   */
  public RowParser getParser() {
    return this.parser;
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
   * Checks to see if value of entry is monotonically increasing.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param entry Current entry in CSV file.
   * @param datum SensorData for a source.
   * @return Updated SensorData for a source.
   */
  public SensorData checkValue(WattDepotClient client, Entry entry, SensorData datum) {
    String isIncreasing = "isMonotonicallyIncreasing";
    Validator monoIncrVal = new MonotonicallyIncreasingValue(client);
    if (!monoIncrVal.validateEntry(entry)) {
      log.log(Level.WARNING, monoIncrVal.getErrorMessage());
      datum.getProperties().getProperty().remove(new Property(isIncreasing, "true"));
      datum.getProperties().getProperty().add(new Property(isIncreasing, "false"));
      entry.setMonotonicallyIncreasing(false);
      this.allNonmonoIncrVals.add(entry);
    }
    return datum;
  }

  /**
   * Sets the sampling interval, either hourly or daily, for a SensorData object.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param entry Current entry in CSV file.
   * @param datum SensorData for a source.
   * @return Updated SensorData for a source.
   */
  public SensorData setSamplingInterval(WattDepotClient client, Entry entry, SensorData datum) {
    Calendar day = Calendar.getInstance();
    int d = entry.getTimestamp().getDay();
    day.set(entry.getTimestamp().getYear(), entry.getTimestamp().getMonth() - 1, d, 0, 0, 0);
    XMLGregorianCalendar start = Tstamp.makeTimestamp(day.getTime().getTime());
    XMLGregorianCalendar end = Tstamp.incrementSeconds(Tstamp.incrementDays(start, 1), -1);

    List<SensorData> data;
    try {
      data = client.getSensorDatas(entry.getSourceName(), start, end);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }

    if (data.size() == 1) {
      datum.getProperties().getProperty().remove(new Property("hourly", "true"));
      this.numDaily++;
    }
    else {
      datum.getProperties().getProperty().remove(new Property("daily", Boolean.toString(true)));
      this.numHourly++;
    }
    return datum;
  }

  /**
   * Adds all sources that have multiple MTU IDs to a list.
   */
  public void getMultipleMtuIds() {
    Collections.sort(this.allSources);

    this.allMtus = new HashSet<Entry>(this.allSources);
    List<Entry> sourceMtus = new ArrayList<Entry>();
    for (Entry e : this.allMtus) {
      sourceMtus.add(e);
    }

    Collections.sort(sourceMtus);

    Set<Entry> mtus = new HashSet<Entry>();
    for (int index = 0; index < sourceMtus.size() - 1; index++) {
      if (sourceMtus.get(index).getSourceName().equals(sourceMtus.get(index + 1).getSourceName())) {
        mtus.add(sourceMtus.get(index));
        mtus.add(sourceMtus.get(index + 1));
      }
    }

    for (Entry e : mtus) {
      this.allDuplicateMtus.add(e);
    }

    Collections.sort(this.allDuplicateMtus);
  }

  /**
   * Prints results of parsing CSV file to standard output and log file.
   * 
   * @param importStartTime Start time of import.
   * @param importEndTime End time of import.
   * @param validateStartTime Start time of validation.
   * @param validateEndTime End time of validation.
   * @param numNoReadings Number of entries in CSV file that have no readings.
   * @param numNonnumericValues Number of entries in CSV file that have non-numeric values.
   * @param numBlankValues Number of entries in CSV file that have blank values.
   */
  public void printStats(long importStartTime, long importEndTime, long validateStartTime,
      long validateEndTime, int numNoReadings, int numNonnumericValues, int numBlankValues) {
    StringBuffer buffer = new StringBuffer();
    String msg = "\n\n==================================================\n";
    buffer.append(msg);
    msg = "Statistics\n";
    buffer.append(msg);
    msg = "--------------------------------------------------\n";
    buffer.append(msg);
    msg = "Filename                           : " + this.filename;
    buffer.append(msg);
    XMLGregorianCalendar startTimestamp = this.entries.get(this.entries.size() - 1).getTimestamp();
    msg = "\n\nFirst Entry Date                   : " + startTimestamp.toString();
    buffer.append(msg);
    XMLGregorianCalendar endTimestamp = this.entries.get(0).getTimestamp();
    buffer.append(msg);
    msg = "\nLast Entry Date                    : " + endTimestamp.toString();
    buffer.append(msg);
    msg = "\n\nEntries Processed                  : " + this.numEntriesProcessed + "\n";
    buffer.append(msg);
    msg = "Invalid Entries                    : " + this.numInvalidEntries + "\n";
    buffer.append(msg);
    msg = "Percentage of Invalid Entries      : ";
    buffer.append(msg);
    double percentage = ((double) this.numInvalidEntries / (double) this.numTotalEntries) * 100.0;
    msg = String.format("%.2f", percentage) + "%\n";
    buffer.append(msg);
    msg = "Total Number of Entries            : " + this.numTotalEntries;
    buffer.append(msg);
    msg = "\n\nBlank Values                       : " + numBlankValues + "\n";
    buffer.append(msg);
    msg = "Non-numeric Values                 : " + numNonnumericValues + "\n";
    buffer.append(msg);
    msg = "No Readings                        : " + numNoReadings + "\n";
    buffer.append(msg);
    msg = "Non-monotonically Increasing Data  : " + this.allNonmonoIncrVals.size() + "\n";
    buffer.append(msg);
    int totalViolations = numNonnumericValues + numNoReadings + numBlankValues;
    totalViolations += this.allNonmonoIncrVals.size();
    msg = "Total Number of Failed Validations : " + totalViolations;
    buffer.append(msg);
    msg = "\n\nNew Sources                        : " + this.numNewSources + "\n";
    buffer.append(msg);
    msg = "Existing Sources                   : " + this.numExistingSources + "\n";
    buffer.append(msg);
    msg = "Total Number of Sources            : " + this.numTotalSources;
    buffer.append(msg);
    msg = "\n\nNumber of Hourly Data              : " + this.numHourly + "\n";
    buffer.append(msg);
    msg = "Number of Daily Data               : " + this.numDaily;
    buffer.append(msg);
    msg = "\n\nNew Data                           : " + this.numNewData + "\n";
    buffer.append(msg);
    msg = "Existing Data                      : " + this.numExistingData + "\n";
    buffer.append(msg);
    msg = "Total Number of Data Imported      : " + (this.numNewData + this.numExistingData);
    buffer.append(msg);
    String runtime = this.getRuntime(importStartTime, importEndTime);
    msg = "\n\nImport Runtime                     : " + runtime + "\n";
    buffer.append(msg);
    runtime = this.getRuntime(validateStartTime, validateEndTime);
    msg = "Validation Runtime                 : " + runtime + "\n";
    buffer.append(msg);
    runtime = this.getRuntime(importStartTime, validateEndTime);
    msg = "Total Runtime                      : " + runtime + "\n\n";
    buffer.append(msg);
    try {
      long numSourcesPerSecond = this.numTotalSources / ((importEndTime - importStartTime) / 1000);
      msg = Long.toString(numSourcesPerSecond);
      if (numSourcesPerSecond > 1) {
        msg += " entries imported per second.\n";
      }
      else {
        msg += " entry imported per second.\n";
      }
      buffer.append(msg);
      numSourcesPerSecond = this.numTotalSources / ((validateEndTime - validateStartTime) / 1000);
      msg = Long.toString(numSourcesPerSecond);
      if (numSourcesPerSecond > 1) {
        msg += " entries validated per second.\n";
      }
      else {
        msg += " entry validated per second.\n";
      }
      buffer.append(msg);
    }
    catch (ArithmeticException e) {
      msg = "Number of entries processed per second is immeasurable.\n";
      buffer.append(msg);
    }
    msg = "\n--------------------------------------------------\n";
    buffer.append(msg);
    if (this.allNonmonoIncrVals.isEmpty()) {
      msg = "\nNo non-monotonically increasing data were found during import.\n";
      buffer.append(msg);
    }
    else {
      msg = "\nNumber of entries with non-monotonically increasing data: ";
      buffer.append(msg);
      msg = this.allNonmonoIncrVals.size() + "\n\n";
      buffer.append(msg);
      String str = null;
      StringBuffer buff = new StringBuffer();
      for (Entry e : this.allNonmonoIncrVals) {
        str = e.toString() + "\n";
        buff.append(str);
      }
      buffer.append(buff.toString());
    }
    msg = "\n--------------------------------------------------\n";
    buffer.append(msg);
    if (this.allDuplicateMtus.isEmpty()) {
      msg = "\nNo sources have multiple MTU IDs.";
      buffer.append(msg);
    }
    else {
      msg = "\nNumber of sources with multiple MTU IDs: " + this.allDuplicateMtus.size() + "\n\n";
      buffer.append(msg);
      String str = null;
      StringBuffer buff = new StringBuffer();
      for (Entry e : this.allDuplicateMtus) {
        str = e.toString();
        if (!e.isMonotonicallyIncreasing()) {
          str += " -- Data from this source and MTU ID is not monotonically increasing.";
        }
        str += "\n";
        buff.append(str);
      }
      buffer.append(buff.toString());
    }
    log.log(Level.INFO, buffer.toString());
    System.out.print(buffer.toString());
  }

  /**
   * Given a CSV file with data for lots of sources, this program will parse each row, create a
   * SensorData object from each, and store the sensor data on a WattDepot server.
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
    HneiCsvImporter inputClient =
        new HneiCsvImporter(filename, serverUri, username, password, true);
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

    long importStartTime = 0;
    long importEndTime = 0;
    long validateStartTime = 0;
    long validateEndTime = 0;
    SensorData datum = null;

    try {
      Entry entry = null;
      int counter = 1;
      String source = null;
      String[] line = null;

      System.out.println("Reading in CSV file...\n");

      importStartTime = Calendar.getInstance().getTimeInMillis();
      // for (int i = 0; i < 10; i++) {
      // line = reader.readNext();
      while ((line = reader.readNext()) != null) {
        source = line[0];
        inputClient.setSourceName(source);
        inputClient.setParser();
        try {
          if ((datum = inputClient.getParser().parseRow(line)) == null) {
            inputClient.numInvalidEntries++;
          }
          else {
            entry = new Entry(source, datum.getProperty("reading"), datum.getTimestamp(), null);
            entry.setMtuId(datum.getProperty("mtuID"));
            inputClient.entries.add(entry);

            if (inputClient.process(client, new Source(source, username, true), datum)) {
              inputClient.numEntriesProcessed++;
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

    // /////////////////////////////////////////////////
    // Done importing file. Now do some post-processing.
    // /////////////////////////////////////////////////
    System.out.print("Checking if readings are monotonically increasing and ");
    System.out.println("are either hourly or daily... This may take a while.");

    int counter = 1;
    validateStartTime = Calendar.getInstance().getTimeInMillis();
    try {
      Entry temp = null;
      for (Entry e : inputClient.entries) {
        datum = client.getSensorData(e.getSourceName(), e.getTimestamp());

        // Check if readings are monotonically increasing.
        datum = inputClient.checkValue(client, e, datum);

        // Classify data as either hourly or daily.
        datum = inputClient.setSamplingInterval(client, e, datum);

        temp = new Entry(e.getSourceName(), null, null, e.getMtuId());
        if (datum.getProperty("isMonotonicallyIncreasing").equals("false")) {
          temp.setMonotonicallyIncreasing(false);
        }
        inputClient.allSources.add(temp);

        // Update sensor data on server.
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

    // Get all sources that have multiple MTU IDs.
    inputClient.getMultipleMtuIds();

    Collections.sort(inputClient.entries, new EntrySortByTimestamp());
    int numNoReadings = ((HneiCsvRowParser) inputClient.getParser()).getNumNoReadings();
    int numNonnumericValues = ((HneiCsvRowParser) inputClient.getParser()).getNumNonnumericValues();
    int numBlankValues = ((HneiCsvRowParser) inputClient.getParser()).getNumBlankValues();

    inputClient.printStats(importStartTime, importEndTime, validateStartTime, validateEndTime,
        numNoReadings, numNonnumericValues, numBlankValues);
  }

}
