package org.wattdepot.hnei.csvimport.hnei;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.hnei.csvimport.Importer;
import org.wattdepot.hnei.csvimport.validation.Entry;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
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
    this.allNonmonoIncrVals = new ArrayList<Entry>();
    this.importStartTime = 0;
    this.importEndTime = 0;
    this.validateStartTime = 0;
    this.validateEndTime = 0;
  }

  /**
   * Sets the parser. Called after setting source name.
   * 
   * @param sourceName Name of a source.
   */
  public void setParser(String sourceName) {
    this.parser = new HneiRowParser(this.toolName, this.serverUri, sourceName, log);
  }

  // TODO: Type of data (hourly or daily) should be a property of source, not sensor data.

  // TODO: Implement a test for monotonicity in CsvImporter after all data has been imported.

  /**
   * Returns the row parser used to parse CSV files from HNEI.
   * 
   * @return The row parser used to parse CSV files from HNEI.
   */
  public HneiRowParser getParser() {
    return (HneiRowParser) this.parser;
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
    msg = "\n\nImport Runtime                     : " + runtime + "\n\n";
    buffer.append(msg);
    try {
      long importRuntime = this.importEndTime - this.importStartTime;
      long numSourcesPerSecond = this.numTotalSources / (importRuntime / 1000);
      msg = Long.toString(numSourcesPerSecond);
      if (numSourcesPerSecond > 1) {
        msg += " entries imported per second.\n\n";
      }
      else {
        msg += " entry imported per second.\n\n";
      }
      buffer.append(msg);
    }
    catch (ArithmeticException e) {
      msg = "Number of entries processed per second is immeasurable.\n\n";
      buffer.append(msg);
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
    Source source = null;

    try {
      int counter = 1;
      String sourceName = null;
      String[] line = null;

      System.out.println("Reading in CSV file [" + this.filename + "]...\n");

      this.importStartTime = Calendar.getInstance().getTimeInMillis();
      while ((line = reader.readNext()) != null) {
        if ((data = this.getParser().parseRow(line)) == null) {
          this.numInvalidEntries++;
        }
        else {
          sourceName = line[2] + "-" + line[3];
          this.setParser(sourceName);

          source = new Source(sourceName, this.username, true);
          source.addProperty(new Property("accountNumber", line[0]));
          source.addProperty(new Property("installDate", line[1]));
          source.addProperty(new Property("meterType", line[4]));

          if (this.process(client, source, data)) {
            this.numEntriesProcessed++;
          }
          else {
            this.numInvalidEntries++;
          }
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

    this.printStats();

    return true;
  }

}
