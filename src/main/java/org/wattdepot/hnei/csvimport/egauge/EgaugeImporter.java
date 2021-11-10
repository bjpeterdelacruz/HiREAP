package org.wattdepot.hnei.csvimport.egauge;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

import org.wattdepot.client.WattDepotClient;
import org.wattdepot.hnei.csvimport.Importer;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import au.com.bytecode.opencsv.CSVReader;

/**
 * This class reads Egauge data from CSV files provided by HNEI (delimited by commas), creates a
 * SensorData object from each line, and stores the SensorData objects to a WattDepot server.
 * 
 * @author BJ Peter DeLaCruz
 */
@Egauge(name = "Egauge", value = "Egauge")
public class EgaugeImporter extends Importer {

  /**
   * Creates a new EgaugeImporter object.
   * 
   * @param filename Name of the CSV file that contains Egauge data.
   * @param uri URI of the WattDepot server.
   * @param username Owner of the WattDepot server.
   * @param password Password to access the WattDepot server.
   * @param skipFirstRow True if first row contains row headers, false otherwise.
   */
  public EgaugeImporter(String filename, String uri, String username, String password,
      boolean skipFirstRow) {
    this.filename = filename;
    this.serverUri = uri;
    this.username = username;
    this.password = password;
    this.skipFirstRow = skipFirstRow;
    this.log = Logger.getLogger(EgaugeImporter.class.getName());
    this.toolName = "EgaugeImporter";
    this.parser = null;
  }

  /**
   * Sets the parser used to parse CSV files.
   * 
   * @param parserName Name of parser.
   */
  public void setParser(String parserName) {
    if ("EgaugeRowParser".equalsIgnoreCase(parserName)) {
      this.parser = new EgaugeRowParser(this.toolName, this.serverUri, null, log);
    }
    else if ("EgaugeRowParserVer2".equalsIgnoreCase(parserName)) {
      this.parser = new EgaugeRowParserVer2(this.toolName, this.serverUri, null, log);
    }
  }

  /**
   * Returns an array of SensorData objects containing energy and power data.
   * 
   * @param sourceName Name of a source.
   * @param data SensorData object containing information for the entire house, including
   * appliances.
   * @return An array of SensorData objects containing energy and power data.
   */
  public SensorData[] getSensorDatas(String sourceName, SensorData data) {
    SensorData[] sensorDatas = new SensorData[4];

    // Whole House
    double power = Double.parseDouble(data.getProperty(SensorData.POWER_CONSUMED));
    Property powerConsumed = new Property(SensorData.POWER_CONSUMED, power);
    String sourceUri = Source.sourceToUri(sourceName, this.serverUri);
    SensorData wholeHouse =
        new SensorData(data.getTimestamp(), this.toolName, sourceUri, powerConsumed);

    // Air Conditioner
    String acPower = data.getProperty("airConditioner").replace(",", "");
    power = Double.parseDouble(acPower);
    powerConsumed = new Property(SensorData.POWER_CONSUMED, power);
    sourceUri = Source.sourceToUri(sourceName + "-airConditioner", this.serverUri);
    SensorData airConditioner =
        new SensorData(data.getTimestamp(), this.toolName, sourceUri, powerConsumed);

    // Water Heater
    String whPower = data.getProperty("waterHeater").replace(",", "");
    power = Integer.parseInt(whPower);
    powerConsumed = new Property(SensorData.POWER_CONSUMED, power);
    sourceUri = Source.sourceToUri(sourceName + "-waterHeater", this.serverUri);
    SensorData waterHeater =
        new SensorData(data.getTimestamp(), this.toolName, sourceUri, powerConsumed);

    // Dryer
    String dryerPower = data.getProperty("dryer").replace(",", "");
    power = Integer.parseInt(dryerPower);
    powerConsumed = new Property(SensorData.POWER_CONSUMED, power);
    sourceUri = Source.sourceToUri(sourceName + "-dryer", this.serverUri);
    SensorData dryer = new SensorData(data.getTimestamp(), this.toolName, sourceUri, powerConsumed);

    sensorDatas[0] = wholeHouse;
    sensorDatas[1] = airConditioner;
    sensorDatas[2] = waterHeater;
    sensorDatas[3] = dryer;

    return sensorDatas;
  }

  /**
   * Prints results of parsing CSV file to standard output and log file.
   */
  @Override
  public void printStats() {
    // TODO: Finish method.
    System.out.println("Number of entries processed: " + this.numEntriesProcessed);
    System.out.println("Number of invalid entries: " + this.numInvalidEntries);
  }

  /**
   * Parses each row, creates a SensorData object from each, and stores the sensor data for a source
   * on a WattDepot server.
   * 
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean processCsvFile() {
    System.out.println("Running EgaugeImporter...");

    // Open CSV file for reading.
    CSVReader reader;
    try {
      int lineno = 1;
      if (!this.skipFirstRow) {
        lineno = 0;
      }
      FileReader fileReader = new FileReader(this.filename, StandardCharsets.UTF_8);
      reader = new CSVReader(fileReader, ',', CSVReader.DEFAULT_QUOTE_CHARACTER, lineno);
    }
    catch (IOException e) {
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

    String mainSourceName = this.filename.substring(0, this.filename.lastIndexOf('.') - 1);
    if (!this.setupLogger() || !this.storeSource(client, mainSourceName)) {
      return false;
    }

    // Create a source for each appliance.
    if (this.getParser() instanceof EgaugeRowParser) {
      String sourceName = mainSourceName + "-airConditioner";
      if (!this.process(client, new Source(sourceName, this.username, true))) {
        return false;
      }
      sourceName = mainSourceName + "-waterHeater";
      if (!this.process(client, new Source(sourceName, this.username, true))) {
        return false;
      }
      sourceName = mainSourceName + "-dryer";
      if (!this.process(client, new Source(sourceName, this.username, true))) {
        return false;
      }
    }

    // Store data on WattDepot server.
    int counter = 0;
    String[] line = null;
    SensorData data = null;

    try {
      if (this.getParser() instanceof EgaugeRowParser) {
        ((EgaugeRowParser) this.getParser()).setSourceName(mainSourceName);
      }
      else {
        ((EgaugeRowParserVer2) this.getParser()).setSourceName(mainSourceName);
      }

      System.out.println("Importing data for source " + mainSourceName + "...");
      // for (int i = 0; i < 100; i++) {
      // line = reader.readNext();
      while ((line = reader.readNext()) != null) {
        if (this.getParser() instanceof EgaugeRowParser) {
          data = ((EgaugeRowParser) this.getParser()).parseRow(line);
        }
        else if (this.getParser() instanceof EgaugeRowParserVer2) {
          data = ((EgaugeRowParserVer2) this.getParser()).parseRow(line);
        }

        if (data == null) {
          this.numInvalidEntries++;
        }
        else {
          if (this.getParser() instanceof EgaugeRowParser) {
            SensorData[] datas = getSensorDatas(mainSourceName, data);

            if (this.process(client, datas[0]) && this.process(client, datas[1])
                && this.process(client, datas[2]) && this.process(client, datas[3])) {
              this.numEntriesProcessed++;
            }
            else {
              this.numInvalidEntries++;
            }
          }
          else if (this.getParser() instanceof EgaugeRowParserVer2) {
            if (this.process(client, data)) {
              this.numEntriesProcessed++;
            }
            else {
              this.numInvalidEntries++;
            }
          }
        }
        this.numTotalEntries++;
        if ((++counter % 500) == 0) {
          System.out.println("Processing line " + counter + " in " + this.filename + "...");
        }
      }
    }
    catch (IOException e) {
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

    // Open CSV file for reading.
    CSVReader reader = null;
    try {
      var fileReader = new FileReader(args[0], StandardCharsets.UTF_8);
      reader = new CSVReader(fileReader, ',', CSVReader.DEFAULT_QUOTE_CHARACTER, 0);
    }
    catch (IOException e) {
      System.err.println("File not found! Exiting...");
      System.exit(1);
    }

    // Get first row in CSV file.
    String[] header = null;
    try {
      header = reader.readNext();
      reader.close();
    }
    catch (IOException e) {
      e.printStackTrace();
      System.exit(1);
    }

    EgaugeImporter client = new EgaugeImporter(filename, serverUri, username, password, true);
    if (header.length == 19 || header.length == 21) {
      client.setParser("EgaugeRowParserVer2");
    }
    else if (header.length == 5 && header[2].contains("AC")) {
      client.setParser("EgaugeRowParser");
    }
    else {
      if (header.length != 5 && header.length != 19 && header.length != 21) {
        System.err.print("Row not correct length: expected 5, 19, or 21. ");
        System.err.println("Got " + header.length + ".");
        System.exit(1);
      }
      System.err.println("Row does not contain column names.");
      System.exit(1);
    }

    if (!client.processCsvFile()) {
      System.exit(1);
    }

    client.printStats();
  }

}
