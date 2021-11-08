package org.wattdepot.hnei.csvimport.hnei;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.datatype.XMLGregorianCalendar;
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

  /** Formats dates that are in the format MM/DD/YYYY hh:mm:ss (A.M.|P.M.). */
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
   * Returns the URI of the WattDepot server.
   * 
   * @return The URI of the WattDepot server.
   */
  public String getServerUri() {
    return this.serverUri;
  }

  /**
   * Parses a row of data for a source from a CSV file provided by HNEI.
   * 
   * @param row Row from a CSV file that contains data.
   * @return SensorData object if parse is successful, null otherwise.
   */
  @Override
  public SensorData parseRow(String[] row) {
    if (row == null) {
      this.log.log(Level.WARNING, "No input row specified.\n");
      return null;
    }

    if (row.length != 9) {
      String msg = "Row not in specified format:\n" + rowToString(row);
      this.log.log(Level.WARNING, msg);
      return null;
    }

    if (row[5].equalsIgnoreCase("No Reading") || row[6].equalsIgnoreCase("No Reading")) {
      String msg = "No reading for source: " + row[0] + "\n" + rowToString(row);
      System.err.print(msg);
      this.log.log(Level.INFO, msg);
      numNoReadings++;
      return null;
    }

    // Run validations on row before adding properties to source in importer class.
    boolean result;
    for (int i = 2; i < row.length; i++) {
      // The eight column is a timestamp, so skip it.
      if (i != 7) {
        for (Validator v : validators) {
          result = v.validateEntry(row[i]);
          if (!result) {
            String msg = "[" + row[i] + "] " + v.getErrorMessage() + "\n" + rowToString(row);
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

    Date readingDate = null;
    try {
      readingDate = formatDateTime.parse(row[7]);
    }
    catch (java.text.ParseException e) {
      try {
        readingDate = formatDate.parse(row[7]);
      }
      catch (java.text.ParseException pe) {
        String msg = "Bad timestamp found in input file: " + row[7] + "\n" + rowToString(row);
        this.log.log(Level.WARNING, msg);
        return null;
      }
    }

    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(readingDate.getTime());
    int energy = Integer.parseInt(row[6]) * 1000; // energy is in kWh
    if (energy < 0) {
      String msg = "[" + energy + "] Energy consumed to date is less than 0!\n" + rowToString(row);
      this.log.log(Level.SEVERE, msg);
      return null;
    }
    Property energyConsumedToDate = new Property(SensorData.ENERGY_CONSUMED_TO_DATE, energy);
    String mtuPort = row[2] + "-" + row[3];
    String sourceUri = Source.sourceToUri(mtuPort, this.serverUri);

    return new SensorData(timestamp, this.toolName, sourceUri, energyConsumedToDate);
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

}
