package org.wattdepot.hnei.csvimport;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This class reads TED data from CSV files provided by HNEI (delimited by commas), creates a
 * SensorData object from each line, and stores the SensorData objects to a WattDepot server.
 * 
 * @author BJ Peter DeLaCruz
 */
public class TedImporter extends HneiImporter {

  /** Log file for this application. */
  private static final Logger log = Logger.getLogger(TedImporter.class.getName());

  /**
   * Creates a new TedImporter object.
   * 
   * @param filename Name of the CSV file that contains TED data.
   * @param uri URI of the WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public TedImporter(String filename, String uri, String username, String password,
      boolean skipFirstRow) {
    super(filename, uri, username, password, skipFirstRow);
    this.toolName = "TedImporter";
    this.parser = new TedRowParser(this.toolName, this.serverUri, null, log);
  }

  /**
   * Prints results of parsing CSV file to standard output and log file.
   */
  @Override
  public void printStats() {
    // TODO: Finish method.
  }

  /**
   * Parses each row, creates a SensorData object from each, and stores the sensor data for a
   * source on a WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean processCsvFile() {
    // Open CSV file for reading.
    CSVReader reader = null;
    try {
      FileReader fileReader = new FileReader(this.filename);
      reader = new CSVReader(fileReader, ',', CSVReader.DEFAULT_QUOTE_CHARACTER, 1);
    }
    catch (FileNotFoundException e) {
      System.err.println("File not found! Exiting...");
      return false;
    }

    // Grab data from CSV file.
    WattDepotClient client = new WattDepotClient(this.serverUri, this.username, this.password);
    if (client.isHealthy() && client.isAuthenticated()) {
      System.out.println("Successfully connected to " + client.getWattDepotUri() + ".");
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
      return false;
    }

    if (!this.setupLogger() || !this.storeSource(client)) {
      return false;
    }

    SensorData datum = null;

    try {
      int counter = 1;
      String[] line = null;

      System.out.println("Reading in CSV file...\n");

      this.importStartTime = Calendar.getInstance().getTimeInMillis();
      // for (int i = 0; i < 25; i++) {
        // line = reader.readNext();
      while ((line = reader.readNext()) != null) {
        ((TedRowParser) this.getParser()).setSourceName(this.sourceName);
        if ((datum = ((TedRowParser) this.getParser()).parseRow(line)) == null) {
          this.numInvalidEntries++;
        }
        else {
          if (this.process(client, datum)) {
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

    return true;
  }

  /**
   * Given a CSV file with data for only one source, this program will parse each row, create a
   * SensorData object from each, and store the sensor data for that source on a WattDepot server.
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

    TedImporter inputClient =
        new TedImporter(filename, serverUri, username, password, true);

    if (!inputClient.processCsvFile()) {
      System.exit(1);
    }
  }

}
