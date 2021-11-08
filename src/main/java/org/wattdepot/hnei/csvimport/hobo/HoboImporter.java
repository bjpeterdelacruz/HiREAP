package org.wattdepot.hnei.csvimport.hobo;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.wattdepot.client.WattDepotClient;
import org.wattdepot.hnei.csvimport.Importer;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This class will export HNEI Hobo data for a source over a given period of time to a CSV file.
 * 
 * @author BJ Peter DeLaCruz
 */
@Hobo(name = "Hobo", value = "Hobo")
public class HoboImporter extends Importer {

  /**
   * Creates a new HoboImporter object.
   * 
   * @param filename File that contains data for sources.
   * @param uri URI for WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public HoboImporter(String filename, String uri, String username, String password,
      boolean skipFirstRow) {
    this.filename = filename;
    this.serverUri = uri;
    this.username = username;
    this.password = password;
    this.skipFirstRow = skipFirstRow;
    this.log = Logger.getLogger(HoboImporter.class.getName());
    this.toolName = "HoboImporter";
    this.parser = new HoboRowParser(this.toolName, this.serverUri, null, log);
  }

  /**
   * Sets the parser. Called after setting source name.
   * 
   * @param sourceName Name of a source.
   */
  public void setParser(String sourceName) {
    this.parser = new HoboRowParser(this.toolName, this.serverUri, sourceName, log);
  }

  /**
   * Prints results of parsing CSV file to standard output and log file.
   */
  @Override
  public void printStats() {
    // TODO: Finish method.
  }

  /**
   * Parses each row, creates a SensorData object from each, and stores the sensor data for a source
   * on a WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean processCsvFile() {
    System.out.println("Running HoboImporter...");

    // Open CSV file for reading.
    CSVReader reader = null;
    try {
      int lineno = 1;
      if (!this.skipFirstRow) {
        lineno = 0;
      }
      FileReader fileReader = new FileReader(this.filename);
      reader = new CSVReader(fileReader, ',', CSVReader.DEFAULT_QUOTE_CHARACTER, lineno);
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

    String sourceName = this.filename.substring(0, this.filename.lastIndexOf('.') - 1);
    if (!this.setupLogger() || !this.storeSource(client, sourceName)) {
      return false;
    }

    SensorData data = null;

    try {
      int counter = 1;
      String[] line = null;

      System.out.println("Reading in CSV file...\n");

      this.importStartTime = Calendar.getInstance().getTimeInMillis();
      for (int i = 0; i < 100; i++) {
        line = reader.readNext();
      // while ((line = reader.readNext()) != null) {
        this.setParser(sourceName);
        if ((data = ((HoboRowParser) this.getParser()).parseRow(line)) == null) {
          this.numInvalidEntries++;
        }
        else {
          if (this.process(client, data)) {
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

    HoboImporter inputClient = new HoboImporter(filename, serverUri, username, password, true);

    if (!inputClient.processCsvFile()) {
      System.exit(1);
    }

  }

}
