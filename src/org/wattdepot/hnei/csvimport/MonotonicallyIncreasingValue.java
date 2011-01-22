package org.wattdepot.hnei.csvimport;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
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

  /** Error message. */
  private static final String errorMessage = "";

  /** Used to make a timestamp in the test program. */
  private SimpleDateFormat formatDateTime;

  /**
   * Creates a new MonotonicallyIncreasingValue object.
   * 
   * @param client Used to grab sensor data from the WattDepot server to use for validation.
   */
  public MonotonicallyIncreasingValue(WattDepotClient client) {
    this.client = client;
    this.formatDateTime = new SimpleDateFormat("hh/MM/yyyy hh:mm:ss a", Locale.US);
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
    String sourceName = ((ReadingData) entry).getSourceName();
    int currReading = Integer.parseInt(((ReadingData) entry).getReading());
    XMLGregorianCalendar currTimestamp = ((ReadingData) entry).getTimestamp();
    XMLGregorianCalendar prevTimestamp = Tstamp.incrementHours(currTimestamp, -2);
    try {
      List<SensorData> sensorDatas =
          client.getSensorDatas(sourceName, prevTimestamp, currTimestamp);
      if (sensorDatas.size() == 1
          && sensorDatas.get(0).getTimestamp().toString().equals(currTimestamp.toString())) {
        // No other data exist but the data at the current timestamp, so return.
        return true;
      }
      SensorData data = sensorDatas.get(0);
      int prevReading = Integer.parseInt(data.getProperty("reading"));
      if (currReading > prevReading) {
        return true;
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
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
        Date timestamp = validator.formatDateTime.parse("1/12/2011 5:35:39 AM");
        currTimestamp = Tstamp.makeTimestamp(timestamp.getTime());
      }
      catch (ParseException e) {
        e.printStackTrace();
        System.exit(1);
      }
      ReadingData currentData = new ReadingData("994702074677", "016635", currTimestamp);
      System.out.println(validator.validateEntry(currentData));
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
    }
  }
}
