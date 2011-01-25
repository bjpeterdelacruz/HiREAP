package org.wattdepot.hnei.csvimport;

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
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * Used to parse CSV files containing sensor data for sources provided by HNEI.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvRowParser extends RowParser {

  /** Log file for the HneiTabularFileSensor application. */
  private Logger log;

  /** Formats dates that are in the format MM/DD/YYYY hh/mm/ss (A.M.|P.M.). */
  private SimpleDateFormat formatDateTime;

  /** Formats dates that are in the format MM/DD/YYYY. */
  private SimpleDateFormat formatDate;

  /** List of validators to verify that entry is valid. */
  private List<Validator> validators;

  /**
   * Creates a new HneiCsvRowParser object.
   * 
   * @param toolName Name of the program.
   * @param serverUri URI of WattDepot server.
   * @param sourceName Source that is described by the sensor data.
   * @param log Log file, created in the HneiTabularFileSensor class.
   */
  public HneiCsvRowParser(String toolName, String serverUri, String sourceName, Logger log) {
    super(toolName, serverUri, sourceName);
    this.formatDateTime = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a", Locale.US);
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
    this.log = log;
    validators = new ArrayList<Validator>();
    validators.add(new NumericValue());
    validators.add(new NonblankValue());
  }

  /**
   * Parses a row of data for a source from a CSV file provided by HNEI.
   * 
   * @param col Row of a CSV file that contains data for one source.
   * @return SensorData object if parse is successful, null otherwise.
   */
  @Override
  public SensorData parseRow(String[] col) {
    if (col == null) {
      this.log.log(Level.WARNING, "No input row specified.\n");
      return null;
    }

    // If some values are missing or no readings were taken, ignore row.

    // Sample row: (comma-delimited)
    // 126580270205 , 10/8/2009 9:48:35 PM , 2144948 , 2 , 215 , 13248 , 13248 ,
    ///////////////// 1/13/2011 8:09:35 AM , -152

    if (col.length != 9) {
      String msg = "Row not in specified format:\n" + rowToString(col);
      this.log.log(Level.WARNING, msg);
      return null;
    }

    if (col[5].equals("No Reading") || col[6].equals("No Reading")) {
      String msg = "No reading for source: " + col[0] + "\n" + rowToString(col);
      System.err.print(msg);
      this.log.log(Level.INFO, msg);
      return null;
    }

    for (int i = 2; i < 9; i++) {
      // col[7] is a timestamp, so skip it.
      if (i != 7) {
        for (Validator v : validators) {
          if (!v.validateEntry(col[i])) {
            String msg = "[" + col[i] + "] " + v.getErrorMessage() + "\n" + rowToString(col);
            System.err.print(msg);
            this.log.log(Level.WARNING, msg);
            return null;
          }
        }
      }
    }

    Properties properties = new Properties();

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
    properties.getProperty().add(new Property("installDate", installTimestamp.toString()));

    properties.getProperty().add(new Property("mtuID", col[2]));
    properties.getProperty().add(new Property("port", col[3]));
    properties.getProperty().add(new Property("meterType", col[4]));
    properties.getProperty().add(new Property("rawRead", col[5]));
    properties.getProperty().add(new Property("reading", col[6]));

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
    properties.getProperty().add(new Property("rssi", col[8]));

    // Update the following properties in the HneiImporter class.
    properties.getProperty().add(new Property("isMonotonicallyIncreasing", "true"));
    properties.getProperty().add(new Property("hourly", "true"));
    properties.getProperty().add(new Property("daily", "true"));

    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(readingDate.getTime());

    return new SensorData(timestamp, this.toolName, Source.sourceToUri(this.sourceName,
        this.serverUri), properties);
  }

  /**
   * Converts a row of entries to one long String.
   * 
   * @param col Array of entries.
   * @return Row from CSV file that did not pass validation.
   */
  private String rowToString(String[] col) {
    String temp = null;
    StringBuffer buffer = new StringBuffer();
    for (String s : col) {
      temp = s + " ";
      buffer.append(temp);
    }
    return buffer.toString() + "\n";
  }
}