package org.wattdepot.hnei.csvimport.validation;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.ResourceNotFoundException;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * A Validator that checks to make sure that the value of an entry is monotonically increasing.
 * 
 * @author BJ Peter DeLaCruz
 */
public class MonotonicallyIncreasingValue implements Validator {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /** Sensor data for a source at a previous timestamp. */
  private SensorData previousData;

  /** Sensor data for a source at the current timestamp. */
  private SensorData currentData;

  /** Used to make a timestamp in the test program. */
  private SimpleDateFormat formatDateTime;

  /**
   * Creates a new MonotonicallyIncreasingValue object.
   * 
   * @param client Used to grab sensor data from the WattDepot server to use for validation.
   */
  public MonotonicallyIncreasingValue(WattDepotClient client) {
    this.client = client;
    this.formatDateTime = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a", Locale.US);
  }

  /**
   * Given a timestamp and reading, this validator checks to make sure that the reading at the last
   * timestamp is not greater than nor equal to the reading at the timestamp that was passed in to
   * this validator.
   * 
   * @param entry The entry to validate.
   * @return True if the entry at the current timestamp is less than the entry at the previous
   * timestamp, false otherwise.
   */
  @Override
  public boolean validateEntry(Object entry) {
    // Unpack
    String sourceName = ((Entry) entry).getSourceName();
    XMLGregorianCalendar currTimestamp = ((Entry) entry).getTimestamp();

    XMLGregorianCalendar prevTimestamp = Tstamp.incrementDays(currTimestamp, -2);
    try {
      List<SensorData> sensorDatas =
          this.client.getSensorDatas(sourceName, prevTimestamp, currTimestamp);
      if (sensorDatas.size() < 2) {
        // No other data exist but the data at the current timestamp, so return.
        return true;
      }

      this.previousData = sensorDatas.get(sensorDatas.size() - 2);
      this.currentData = client.getSensorData(sourceName, currTimestamp);
      double currReading = this.currentData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
      double prevReading =
          this.previousData.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE);
      if (currReading >= prevReading) {
        return true;
      }
    }
    catch (ResourceNotFoundException e) {
      // Source is being added for the first time.
      return true;
    }
    catch (WattDepotClientException e) {
      return true;
    }
    return false;
  }

  /**
   * Returns a string explaining why validation failed for an entry.
   * 
   * @return An error message.
   */
  @Override
  public String getErrorMessage() {
    String reading = "reading";
    String errorMessage = "The reading at " + this.previousData.getTimestamp();
    errorMessage += " (" + this.previousData.getProperty(reading) + ") is greater than or equal ";
    errorMessage += "to the reading at " + this.currentData.getTimestamp() + " (";
    errorMessage += this.currentData.getProperty(reading) + ") for ";
    errorMessage += this.currentData.getSource() + ".";
    return errorMessage;
  }

  /**
   * Test program to see if validator works.
   * 
   * @param args Information used to connect to WattDepot server.
   */
  public static void main(String[] args) {
    if (args.length != 3) {
      System.err.println("Not enough arguments. Exiting...");
      System.exit(1);
    }

    WattDepotClient client = new WattDepotClient(args[0], args[1], args[2]);
    if (client.isHealthy() && client.isAuthenticated()) {
      MonotonicallyIncreasingValue validator = new MonotonicallyIncreasingValue(client);
      XMLGregorianCalendar currTimestamp = null;
      try {
        Date timestamp = validator.formatDateTime.parse("2/4/2011 5:33:44 PM");
        currTimestamp = Tstamp.makeTimestamp(timestamp.getTime());
      }
      catch (ParseException e) {
        e.printStackTrace();
        System.exit(1);
      }
      Entry currentData = new Entry("1770606-1", "27381000", currTimestamp, null); // 027385
      System.out.println(validator.validateEntry(currentData));
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
    }
  }
}
