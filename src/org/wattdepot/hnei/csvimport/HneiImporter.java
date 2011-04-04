package org.wattdepot.hnei.csvimport;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.RowParseException;
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
@Hnei(name = "Hnei", value = "Hnei")
public class HneiImporter extends Importer {

  /**
   * Creates a new HneiImporter object.
   * 
   * @param filename File that contains data for sources.
   * @param uri URI for WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public HneiImporter(String filename, String uri, String username, String password,
      boolean skipFirstRow) {
    this.log = Logger.getLogger(HneiImporter.class.getName());
    this.filename = filename;
    this.serverUri = uri;
    this.sourceName = null;
    this.username = username;
    this.password = password;
    this.skipFirstRow = skipFirstRow;
    this.toolName = "HneiImporter";
    this.parser = new HneiRowParser(this.toolName, this.serverUri, null, log);
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
    this.importStartTime = 0;
    this.importEndTime = 0;
    this.validateStartTime = 0;
    this.validateEndTime = 0;
  }

  /**
   * Sets the parser. Called after setting source name.
   */
  public void setParser() {
    this.parser = new HneiRowParser(this.toolName, this.serverUri, this.sourceName, log);
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
   * Checks to see if value of entry is monotonically increasing.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param entry Current entry in CSV file.
   * @param data SensorData for a source.
   * @return Updated SensorData for a source.
   */
  public SensorData setMonotonicallyIncreasingProperty(WattDepotClient client, Entry entry,
      SensorData data) {
    String isIncreasing = "isMonotonicallyIncreasing";
    Validator monoIncrVal = new MonotonicallyIncreasingValue(client);
    if (monoIncrVal.validateEntry(entry)) {
      data.getProperties().getProperty().add(new Property(isIncreasing, "true"));
    }
    else {
      log.log(Level.WARNING, monoIncrVal.getErrorMessage());
      data.getProperties().getProperty().add(new Property(isIncreasing, "false"));
      entry.setMonotonicallyIncreasing(false);
      this.allNonmonoIncrVals.add(entry);
    }
    return data;
  }

  /**
   * Sets the sampling interval, either hourly or daily, for a SensorData object.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param entry Current entry in CSV file.
   * @param data SensorData for a source.
   * @return Updated SensorData object for a source.
   */
  public SensorData setSamplingIntervalProp(WattDepotClient client, Entry entry, SensorData data) {
    Calendar day = Calendar.getInstance();
    int d = entry.getTimestamp().getDay();
    day.set(entry.getTimestamp().getYear(), entry.getTimestamp().getMonth() - 1, d, 0, 0, 0);
    // e.g. 8/1/2009 12:00:00 AM
    XMLGregorianCalendar start = Tstamp.makeTimestamp(day.getTime().getTime());
    // e.g. 8/1/2009 11:59:59 PM
    XMLGregorianCalendar end = Tstamp.incrementSeconds(Tstamp.incrementDays(start, 1), -1);

    List<SensorData> datas;
    try {
      datas = client.getSensorDatas(entry.getSourceName(), start, end);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }

    if (datas.size() == 1) {
      data.getProperties().getProperty().remove(new Property("hourly", "true"));
      this.numDaily++;
    }
    else {
      data.getProperties().getProperty().remove(new Property("daily", Boolean.toString(true)));
      this.numHourly++;
    }
    return data;
  }

  /**
   * Adds energy and power data to a SensorData object.
   * 
   * @param client WattDepotClient used to connect to the WattDepot server.
   * @param entry Current entry in CSV file.
   * @param data SensorData for a source.
   * @return Updated SensorData object for a source.
   */
  public SensorData addProperties(WattDepotClient client, Entry entry, SensorData data) {
    List<SensorData> datas = null;
    try {
      XMLGregorianCalendar prevTimestamp = Tstamp.incrementDays(data.getTimestamp(), -2);
      datas = client.getSensorDatas(entry.getSourceName(), prevTimestamp, data.getTimestamp());
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return null;
    }

    if (datas.size() > 1) {
      SensorData prevData = datas.get(datas.size() - 2);
      double prevEnergy = prevData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
      double currEnergy = data.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
      double energy = Math.abs(prevEnergy - currEnergy);
      data.addProperty(new Property(SensorData.ENERGY_CONSUMED, energy));

      long prevTimestamp = prevData.getTimestamp().toGregorianCalendar().getTimeInMillis();
      long currTimestamp = data.getTimestamp().toGregorianCalendar().getTimeInMillis();
      double hours = Math.abs(prevTimestamp - currTimestamp) / 1000.0 / 60.0 / 60.0;
      if (hours > 0) {
        double power = energy / hours;
        data.addProperty(new Property(SensorData.POWER_CONSUMED, power));
      }
    }

    return data;
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
   * Creates a CSV file that contains sources with multiple MTU IDs.
   * 
   * @return True if successful, false otherwise.
   */
  public boolean generateCsvFile() {
    String today = Calendar.getInstance().getTime().toString().replaceAll("[ :]", "_");
    File outputFile = new File(today + ".csv");
    outputFile.setWritable(true);

    BufferedWriter writer = null;
    try {
      writer = new BufferedWriter(new FileWriter(outputFile));
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    String str = null;
    StringBuffer buffer = new StringBuffer();
    str = "Account Number,MTU ID\n";
    buffer.append(str);
    for (Entry e : this.allDuplicateMtus) {
      buffer.append(e.getSourceName());
      str = "," + e.getMtuId() + "\n";
      buffer.append(str);
    }

    try {
      writer.write(buffer.toString());
      writer.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      return false;
    }

    return true;
  }

  /**
   * Prints results of parsing CSV file to standard output and log file.
   */
  @Override
  public void printStats() {
    StringBuffer buffer = new StringBuffer();
    String msg = "\n\n==================================================\n";
    buffer.append(msg);
    msg = "Statistics\n";
    buffer.append(msg);
    msg = "--------------------------------------------------\n";
    buffer.append(msg);
    msg = "Filename                           : " + this.filename;
    buffer.append(msg);
    if (!this.entries.isEmpty()) {
      Entry entry = this.entries.get(this.entries.size() - 1);
      XMLGregorianCalendar startTimestamp = entry.getTimestamp();
      msg = "\n\nFirst Entry Date                   : " + startTimestamp.toString();
      buffer.append(msg);
      XMLGregorianCalendar endTimestamp = this.entries.get(0).getTimestamp();
      buffer.append(msg);
      msg = "\nLast Entry Date                    : " + endTimestamp.toString();
      buffer.append(msg);
    }
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
    int numBlankValues = ((HneiRowParser) this.parser).getNumBlankValues();
    msg = "\n\nBlank Values                       : " + numBlankValues + "\n";
    buffer.append(msg);
    int numNonnumericValues = ((HneiRowParser) this.parser).getNumNonnumericValues();
    msg = "Non-numeric Values                 : " + numNonnumericValues + "\n";
    buffer.append(msg);
    int numNoReadings = ((HneiRowParser) this.parser).getNumNoReadings();
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
    String runtime = this.getRuntime(this.importStartTime, this.importEndTime);
    msg = "\n\nImport Runtime                     : " + runtime + "\n";
    buffer.append(msg);
    runtime = this.getRuntime(this.validateStartTime, this.validateEndTime);
    msg = "Validation Runtime                 : " + runtime + "\n";
    buffer.append(msg);
    if (this.numEntriesProcessed > 0) {
      runtime = this.getRuntime(this.importStartTime, this.validateEndTime);
      msg = "Total Runtime                      : " + runtime + "\n\n";
    }
    else {
      msg = "Total Runtime                      : 00:00:00\n\n";
    }
    buffer.append(msg);
    try {
      long importRuntime = this.importEndTime - this.importStartTime;
      long numSourcesPerSecond = this.numTotalSources / (importRuntime / 1000);
      msg = Long.toString(numSourcesPerSecond);
      if (numSourcesPerSecond > 1) {
        msg += " entries imported per second.\n";
      }
      else {
        msg += " entry imported per second.\n";
      }
      buffer.append(msg);
      long validateRuntime = this.validateEndTime - this.validateStartTime;
      numSourcesPerSecond = this.numTotalSources / (validateRuntime / 1000);
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
      msg = "\nNo sources have multiple MTU IDs.\n\n";
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
   * Parses each row, creates a SensorData object from each, and stores the sensor data on a
   * WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean processCsvFile() {
    System.out.println("Running HneiImporter...");

    // Open CSV file for reading.
    CSVReader reader = null;
    try {
      int line = 1;
      if (!this.skipFirstRow) {
        line = 0;
      }
      char defaultChar = CSVReader.DEFAULT_QUOTE_CHARACTER;
      reader = new CSVReader(new FileReader(this.filename), ',', defaultChar, line);
    }
    catch (FileNotFoundException e) {
      System.err.println("File not found! Exiting...");
      return false;
    }

    // Grab data from CSV file.
    WattDepotClient client = new WattDepotClient(serverUri, username, password);
    if (client.isHealthy() && client.isAuthenticated()) {
      System.out.println("Successfully connected to " + client.getWattDepotUri() + ".");
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
      return false;
    }

    if (!this.setupLogger()) {
      return false;
    }

    SensorData data = null;

    try {
      Entry entry = null;
      int counter = 1;
      String source = null;
      String[] line = null;

      System.out.println("Reading in CSV file [" + this.filename + "]...\n");

      this.importStartTime = Calendar.getInstance().getTimeInMillis();
      // for (int i = 0; i < 1000; i++) {
      // line = reader.readNext();
      while ((line = reader.readNext()) != null) {
        try {
          if ((data = this.getParser().parseRow(line)) == null) {
            this.numInvalidEntries++;
          }
          else {
            source = line[2] + "-" + line[3];
            this.setSourceName(source);
            this.setParser();
            entry = new Entry(source, data.getProperty("reading"), data.getTimestamp(), null);
            entry.setMtuId(data.getProperty("mtuID"));
            this.entries.add(entry);

            if (this.process(client, new Source(source, this.username, true), data)) {
              this.numEntriesProcessed++;
            }
            else {
              this.numInvalidEntries++;
            }
          }
        }
        catch (RowParseException e) {
          log.log(Level.SEVERE, "There was a problem parsing the entry for source " + source);
          this.numInvalidEntries++;
        }
        this.numTotalEntries++;
        if ((++counter % 500) == 0) {
          System.out.println("Processing line " + counter + " in " + this.filename + "...");
        }
      }
      this.importEndTime = Calendar.getInstance().getTimeInMillis();
    }
    catch (IOException e) {
      String msg = "There was a problem reading in the input file:\n" + e.toString();
      msg += "\n\nExiting...";
      System.err.println(msg);
      log.log(Level.SEVERE, msg);
      return false;
    }

    if (this.numEntriesProcessed == 0) {
      String msg = "No entries were processed.";
      log.log(Level.SEVERE, msg);
      System.err.println(msg);
      this.printStats();
      return true;
    }

    // /////////////////////////////////////////////////
    // Done importing file. Now do some post-processing.
    // /////////////////////////////////////////////////
    System.out.print("Checking if readings are monotonically increasing and ");
    System.out.println("are either hourly or daily... This may take a while.");

    int counter = 1;
    this.validateStartTime = Calendar.getInstance().getTimeInMillis();
    try {
      Entry temp = null;
      for (Entry e : this.entries) {
        data = client.getSensorData(e.getSourceName(), e.getTimestamp());

        // Check if readings are monotonically increasing.
        if (data.getProperty("isMonotonicallyIncreasing") == null) {
          data = this.setMonotonicallyIncreasingProperty(client, e, data);
        }
        else {
          continue;
        }

        // Classify data as either hourly or daily.
        data = this.setSamplingIntervalProp(client, e, data);
        if (data == null) {
          return false;
        }

        temp = new Entry(e.getSourceName(), null, null, e.getMtuId());
        if (data.getProperty("isMonotonicallyIncreasing").equals("false")) {
          temp.setMonotonicallyIncreasing(false);
        }
        this.allSources.add(temp);

        // Add energyConsumed.
        data = this.addProperties(client, e, data);
        if (data == null) {
          return false;
        }

        // Update sensor data on server.
        client.deleteSensorData(e.getSourceName(), e.getTimestamp());
        client.storeSensorData(data);

        if (++counter % 500 == 0) {
          System.out.println("Processing entry " + counter + "...");
        }
      }
      this.validateEndTime = Calendar.getInstance().getTimeInMillis();
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }
    catch (JAXBException e) {
      e.printStackTrace();
      return false;
    }

    // Get all sources that have multiple MTU IDs.
    this.getMultipleMtuIds();

    Collections.sort(this.entries, new EntrySortByTimestamp());

    this.printStats();

    if (!this.allDuplicateMtus.isEmpty() && !this.generateCsvFile()) {
      return false;
    }

    return true;

  }

  /**
   * Given a CSV file with data for lots of sources, this program will parse each row, create a
   * SensorData object from each, and store the sensor data on a WattDepot server.
   * 
   * @param args Contains filename, server URI, username, and password.
   */
  public static void main(String[] args) {
    if (args.length < 3 || args.length > 4) {
      System.err.println("Command-line arguments not in correct format. Exiting...");
      System.exit(1);
    }

    String serverUri = args[0];
    String username = args[1];
    String password = args[2];

    boolean processAllFiles = false;
    if (args.length == 4 && "-all".equalsIgnoreCase(args[3])) {
      processAllFiles = true;
    }
    else if (args.length == 4 && !"-all".equalsIgnoreCase(args[3])) {
      System.err.println("Illegal flag for fourth parameter. Expected [-all], got [" + args[3]
          + "].");
      System.exit(1);
    }

    boolean processNextFile = true;
    String response = "";
    String dirName = System.getProperties().getProperty("user.dir");
    String[] children = Importer.getAllCsvFiles(dirName);

    for (int index = 0; index < children.length; index++) {
      if (processNextFile) {
        HneiImporter inputClient =
            new HneiImporter(children[index], serverUri, username, password, true);

        System.out.println("Processing " + children[index] + "...");
        if (!inputClient.processCsvFile() && !inputClient.closeLogger()) {
          System.exit(1);
        }
      }

      if (!processAllFiles) {
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

}
