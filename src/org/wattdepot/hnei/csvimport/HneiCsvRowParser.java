package org.wattdepot.hnei.csvimport;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.datainput.RowParser;
import org.wattdepot.hnei.csvimport.validation.NumericValue;
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
    this.formatDateTime = new SimpleDateFormat("hh/MM/yyyy hh:mm:ss a", Locale.US);
    this.formatDate = new SimpleDateFormat("hh/MM/yyyy", Locale.US);
    this.log = log;
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

    NumericValue validateNumber = new NumericValue();

    // If some values are missing or no readings were taken, ignore row.

    // Sample row: (comma-delimited)
    // 126580270205 , 10/8/2009 9:48:35 PM , 2144948 , 2 , 215 , 13248 , 13248 ,
    // 1/13/2011 8:09:35 AM , -152

    if (col.length != 9) {
      this.log.log(Level.WARNING, "Row not in specified format. Skipping entry...\n");
      return null;
    }

    if (col[5].equals("No Reading") || col[6].equals("No Reading")) {
      String msg = "No reading for source " + this.sourceName + ". Skipping entry...\n";
      this.log.log(Level.INFO, msg);
      return null;
    }

    Properties properties = new Properties();
    // properties.getProperty().add(new Property("account", col[0]));

    Date installDate = null;
    try {
      installDate = formatDateTime.parse(col[1]);
    }
    catch (java.text.ParseException e) {
      try {
        installDate = formatDate.parse(col[1]);
      }
      catch (java.text.ParseException pe) {
        this.log.log(Level.WARNING, "Bad timestamp found in input file: " + col[1] + "\n");
        return null;
      }
    }
    XMLGregorianCalendar installTimestamp = Tstamp.makeTimestamp(installDate.getTime());
    properties.getProperty().add(new Property("installDate", installTimestamp.toString()));

    if (validateNumber.validateEntry(col[2])) {
      properties.getProperty().add(new Property("mtuID", col[2]));
    }
    else {
      return null;
    }

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
        this.log.log(Level.WARNING, "Bad timestamp found in input file: " + col[7] + "\n");
        return null;
      }
    }
    properties.getProperty().add(new Property("rssi", col[8]));

    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(readingDate.getTime());
    return new SensorData(timestamp, this.toolName, Source.sourceToUri(this.sourceName,
        this.serverUri), properties);
  }

}
