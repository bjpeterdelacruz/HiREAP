package org.wattdepot.hnei.csvimport;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.hnei.csvimport.validation.NonblankValue;
import org.wattdepot.hnei.csvimport.validation.NumericValue;
import org.wattdepot.hnei.csvimport.validation.Validator;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used to parse CSV files containing TED data for one source provided by HNEI.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HneiCsvTedRowParser extends HneiCsvRowParser {

  /**
   * Creates a new HneiCsvTedRowParser object.
   * 
   * @param toolName Name of the program.
   * @param serverUri URI of WattDepot server.
   * @param sourceName Source that is described by the sensor data.
   * @param log Log file, created in the HneiTabularFileSensor class.
   */
  public HneiCsvTedRowParser(String toolName, String serverUri, String sourceName, Logger log) {
    super(toolName, serverUri, sourceName, log);
    this.formatDateTime = new SimpleDateFormat("MM/dd/yy hh:mm a", Locale.US);
  }

  /**
   * Parses a row of TED data for a source from a CSV file provided by HNEI.
   * 
   * @param col Row from a CSV file that contains TED data.
   * @return SensorData object if parse is successful, null otherwise.
   */
  public SensorData parseRow(String[] col) {
    if (col == null) {
      this.log.log(Level.WARNING, "No input row specified.\n");
      return null;
    }

    if (col.length != 25) {
      String msg = "Row not in specified format:\n" + rowToString(col);
      this.log.log(Level.WARNING, msg);
      return null;
    }

    if (col[1].equals("null")) {
      String msg = "No reading for source at " + col[0] + ".\n";
      System.err.print(msg);
      this.log.log(Level.INFO, msg);
      numNoReadings++;
      return null;
    }

    col[1] = col[1].replace(",", "");

    // Run validations.
    boolean result;
    // Validate only MTU1, which is the whole house.
    for (Validator v : validators) {
      result = v.validateEntry(col[1]);
      if (!result) {
        String msg = "[" + col[1] + "] " + v.getErrorMessage() + "\n" + rowToString(col);
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

    Date date = null;
    try {
      date = formatDateTime.parse(col[0]);
    }
    catch (java.text.ParseException e) {
      try {
        date = formatDate.parse(col[0]);
      }
      catch (java.text.ParseException pe) {
        String msg = "Bad timestamp found in input file: " + col[0] + "\n" + rowToString(col);
        this.log.log(Level.WARNING, msg);
        return null;
      }
    }
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(date.getTime());

    Property powerConsumed = new Property(SensorData.POWER_CONSUMED, Integer.parseInt(col[1]));
    String sourceUri = Source.sourceToUri(this.sourceName, this.serverUri);
    SensorData datum = new SensorData(timestamp, this.toolName, sourceUri, powerConsumed);

    datum.addProperty(new Property("mtu2", getReading(col[2])));
    datum.addProperty(new Property("mtu3", getReading(col[3])));
    datum.addProperty(new Property("mtu4", getReading(col[4])));
    datum.addProperty(new Property("other", getReading(col[13])));
    String status = "true";
    if (col[21].equals("0")) {
      status = "false";
    }
    datum.addProperty(new Property("isAirConditionerOff", status));
    datum.addProperty(new Property("blank", col[22]));
    datum.addProperty(new Property("not blank", col[23]));

    return datum;
  }

  /**
   * Returns "N/A" if there is no data or the String itself if there is data.
   * 
   * @param str "null" or data.
   * @return "N/A" if there is no data or the String itself if there is data.
   */
  private String getReading(String str) {
    if ("null".equals(str)) {
      return "N/A";
    }
    else {
      return str;
    }
  }

}
