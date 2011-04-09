package org.wattdepot.hnei.csvimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.hnei.csvimport.validation.NonblankValue;
import org.wattdepot.hnei.csvimport.validation.NumericValue;
import org.wattdepot.hnei.csvimport.validation.Validator;
import org.wattdepot.resource.property.jaxb.Properties;
import org.wattdepot.resource.property.jaxb.Property;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.resource.source.jaxb.Source;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used to parse CSV files containing Hobo data for one source provided by HNEI.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HoboRowParser extends HneiRowParser {

  /**
   * Creates a new HoboRowParser object.
   * 
   * @param toolName Name of the program.
   * @param serverUri URI of WattDepot server.
   * @param sourceName Source that is described by the sensor data.
   * @param log Log file, created in the HneiTabularFileSensor class.
   */
  public HoboRowParser(String toolName, String serverUri, String sourceName, Logger log) {
    super(toolName, serverUri, sourceName, log);
    this.formatDateTime = new SimpleDateFormat("MM/dd/yyyy kk:mm", Locale.US);
  }

  /**
   * Parses a row of Hobo data for a source from a CSV file provided by HNEI.
   * 
   * @param col Row from a CSV file that contains Hobo data.
   * @return SensorData object if parse is successful, null otherwise.
   */
  @Override
  public SensorData parseRow(String[] col) {
    if (col == null) {
      this.log.log(Level.WARNING, "No input row specified.\n");
      return null;
    }

    if (col.length < 5 || col.length == 6 || col.length == 8 || col.length > 9) {
      String msg = "Row not in specified format:\n" + rowToString(col);
      this.log.log(Level.WARNING, msg);
      return null;
    }

    // Run validations.
    boolean result;
    for (int i = 2; i < 5; i++) {
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

    Date date = null;
    try {
      date = formatDateTime.parse(col[1]);
    }
    catch (ParseException e) {
      try {
        date = formatDate.parse(col[1]);
      }
      catch (ParseException pe) {
        String msg = "Bad timestamp found in input file: " + col[1] + "\n" + rowToString(col);
        this.log.log(Level.WARNING, msg);
        return null;
      }
    }
    XMLGregorianCalendar timestamp = Tstamp.makeTimestamp(date.getTime());

    // Property powerConsumed = new Property(SensorData.POWER_CONSUMED, Integer.parseInt(col[1]));
    String sourceUri = Source.sourceToUri(this.sourceName, this.serverUri);
    SensorData data = new SensorData(timestamp, this.toolName, sourceUri, new Properties());

    data.addProperty(new Property("tempF", col[2]));
    data.addProperty(new Property("rh%", col[3]));
    data.addProperty(new Property("lumenPerSqFt", col[4]));

    return data;
  }

}
