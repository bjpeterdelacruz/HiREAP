package org.wattdepot.hnei.csvimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.bind.JAXBException;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.datainput.RowParser;
import org.wattdepot.hnei.csvimport.validation.NonblankValue;
import org.wattdepot.hnei.csvimport.validation.NumericValue;
import org.wattdepot.hnei.csvimport.validation.Validator;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used to parse CSV files containing data for sources provided by HNEI.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiRowParser extends RowParser {

  /** Log file for the HneiTabularFileSensor application. */
  protected Logger log;

  /** Formats dates that are in the format MM/DD/YYYY hh/mm/ss (A.M.|P.M.). */
  protected SimpleDateFormat formatDateTime;

  /** Formats dates that are in the format MM/DD/YYYY. */
  protected SimpleDateFormat formatDate;

  /** List of validators to verify that entry is valid. */
  protected List<Validator> validators;

  /** Total number of entries with no readings. */
  protected static int numNoReadings = 0;

  /** Total number of entries with non-numeric data. */
  protected static int numNonnumericValues = 0;

  /** Total number of entries with missing data. */
  protected static int numBlankValues = 0;

  /**
   * Creates a new HneiRowParser object.
   * 
   * @param toolName Name of the program.
   * @param serverUri URI of WattDepot server.
   * @param sourceName Source that is described by the sensor data.
   * @param log Log file, created in the HneiTabularFileSensor class.
   */
  public HneiRowParser(String toolName, String serverUri, String sourceName, Logger log) {
    super(toolName, serverUri, sourceName);
    this.formatDateTime = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a", Locale.US);
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.log = log;
    this.validators = new ArrayList<Validator>();
    this.validators.add(new NonblankValue());
    this.validators.add(new NumericValue());
  }

  /**
   * Sets the name of a source.
   * 
   * @param sourceName Sets the name of a source.
   */
  public void setSourceName(String sourceName) {
    this.sourceName = sourceName;
  }

  /**
   * Returns the total number of entries with no readings.
   * 
   * @return The total number of entries with no readings.
   */
  public int getNumNoReadings() {
    return numNoReadings;
  }

  /**
   * Returns the total number of entries with non-numeric data.
   * 
   * @return The total number of entries with non-numeric data.
   */
  public int getNumNonnumericValues() {
    return numNonnumericValues;
  }

  /**
   * Returns the total number of entries with missing data.
   * 
   * @return The total number of entries with missing data.
   */
  public int getNumBlankValues() {
    return numBlankValues;
  }

  /**
   * Parses a row of data for a source from a CSV file provided by HNEI.
   * 
   * @param col Row from a CSV file that contains data.
   * @return SensorData object if parse is successful, null otherwise.
   */
  @Override
  public SensorData parseRow(String[] col) {
    if (col == null) {
      this.log.log(Level.WARNING, "No input row specified.\n");
      return null;
    }

    if (col.length != 9) {
      String msg = "Row not in specified format:\n" + rowToString(col);
      this.log.log(Level.WARNING, msg);
      return null;
    }

    if (col[5].equals("No Reading") || col[6].equals("No Reading")) {
      String msg = "No reading for source: " + col[0] + "\n" + rowToString(col);
      System.err.print(msg);
      this.log.log(Level.INFO, msg);
      numNoReadings++;
      return null;
    }

    // Run validations.
    boolean result;
    for (int i = 2; i < col.length; i++) {
      // The eight column is a timestamp, so skip it.
      if (i != 7) {
        for (Validator v : validators) {
          result = v.validateEntry(col[i]);
          if (!result) {
            String msg = "[" + col[i] + "] " + v.getErrorMessage() + "\n" + rowToString(col);
            System.err.print(msg);
            this.log.log(Level.WARNING, msg);
          }
          if (v instanceof NonblankValue && !result) {
            numBlankValues++;
            return null;
          }
          if (v instanceof NumericValue && !result) {
            numNonnumericValues++;
            return null;
          }
        }
      }
    }

    Date installDate = null;
    try {
      installDate = formatDateTime.parse(col[1]);
    }
    catch (java.text.ParseException e) {
      try {
        installDate = formatDate.parse(col[1]);
      }
      catch (java.text.ParseException pe) {
        String msg = "Bad timestamp found in input file: " + col[1] + "\n" + rowToString(col);
        this.log.log(Level.WARNING, msg);
        return null;
      }
    }
    XMLGregorianCalendar installTimestamp = Tstamp.makeTimestamp(installDate.getTime());

    Date readingDate = null;
    try {
      readingDate = formatDateTime.parse(col[7]);
    }
    catch (java.text.ParseException e) {
      try {
        readingDate = formatDate.parse(col[7]);
      }
      catch (java.text.ParseException pe) {
        String msg = "Bad timestamp found in input file: " + col[7] + "\n" + rowToString(col);
        this.log.log(Level.WARNING, msg);
        return null;
      }
    }

    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(readingDate.getTime());
    int energy = Integer.parseInt(col[6]) * 1000;
    Property energyConsumedToDate = new Property(SensorData.ENERGY_CONSUMED_TO_DATE, energy);
    String mtuPort = col[2] + "-" + col[3];
    String sourceUri = Source.sourceToUri(mtuPort, this.serverUri);
    SensorData datum = new SensorData(timestamp, this.toolName, sourceUri, energyConsumedToDate);

    datum.addProperty(new Property("accountNumber", col[0]));
    datum.addProperty(new Property("installDate", installTimestamp.toString()));
    datum.addProperty(new Property("mtuID", col[2]));
    datum.addProperty(new Property("port", col[3]));
    datum.addProperty(new Property("meterType", col[4]));
    datum.addProperty(new Property("rawRead", col[5]));
    datum.addProperty(new Property("reading", col[6]));
    datum.addProperty(new Property("rssi", col[8]));
    datum.addProperty(new Property("hourly", "true"));
    datum.addProperty(new Property("daily", "true"));

    return datum;
  }

  /**
   * Converts a row of entries to one long String.
   * 
   * @param col Array of entries.
   * @return Row from CSV file that did not pass validation.
   */
  public String rowToString(String[] col) {
    String temp = null;
    StringBuffer buffer = new StringBuffer();
    for (String s : col) {
      temp = s + " ";
      buffer.append(temp);
    }
    return buffer.toString() + "\n";
  }

  /**
   * Test program to see if row parser works.
   * 
   * @param args URI, username, and password to connect to WattDepot server.
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Command-line arguments not in correct format. Exiting...");
      System.exit(1);
    }

    String serverUri = args[0];
    String username = args[1];
    String password = args[2];

    WattDepotClient client = new WattDepotClient(serverUri, username, password);
    if (!client.isHealthy() || !client.isAuthenticated()) {
      System.err.println("Unable to connect to WattDepot server.");
      System.exit(1);
    }
    System.out.println("Successfully connected to " + client.getWattDepotUri() + ".\n");

    String sourceName = "1951005-1";
    HneiRowParser parser =
        new HneiRowParser("HneiCsvRowParser", serverUri, sourceName, null);
    String[] col1 =
        { "994515990077", "8/1/2009", "1951005", "1", "491", "35958", "035958",
            "1/1/2011 9:00:00 AM", "0" };
    String[] col2 =
        { "994515990077", "8/1/2009", "1951005", "1", "491", "35955", "035955",
            "1/1/2011 8:00:00 AM", "0" };

    SensorData datum1 = null;
    SensorData datum2 = null;
    try {
      client.storeSource(new Source(sourceName, username, true), true);
      datum1 = parser.parseRow(col1);
      System.out.println(datum1);
      client.storeSensorData(datum1);
      datum2 = parser.parseRow(col2);
      System.out.println(datum2);
      client.storeSensorData(datum2);
    }
    catch (WattDepotClientException e) {
      System.err.println(e);
    }
    catch (JAXBException e) {
      e.printStackTrace();
      System.exit(1);
    }

    Date date1 = null;
    Date date2 = null;
    try {
      date1 = parser.formatDateTime.parse("1/1/2011 8:00:00 AM");
      date2 = parser.formatDateTime.parse("1/1/2011 9:00:00 AM");
    }
    catch (ParseException e) {
      e.printStackTrace();
      System.exit(1);
    }
    XMLGregorianCalendar timestamp1 = Tstamp.makeTimestamp(date1.getTime());
    XMLGregorianCalendar timestamp2 = Tstamp.makeTimestamp(date2.getTime());

    System.out.println("\nData:");
    try {
      System.out.println(client.getSensorData(sourceName, timestamp1));
      System.out.println(client.getSensorData(sourceName, timestamp2));
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      System.exit(1);
    }
  }

}
