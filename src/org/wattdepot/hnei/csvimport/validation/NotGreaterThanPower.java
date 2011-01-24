package org.wattdepot.hnei.csvimport.validation;

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
 * A Validator that checks to make sure that the value of an entry is not greater than the maximum
 * power that is allowed.
 * 
 * @author BJ Peter DeLaCruz
 */
public class NotGreaterThanPower implements Validator {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /** Sensor data for a source at the current timestamp. */
  private SensorData currentData;

  /** Used to make a timestamp in the test program. */
  private SimpleDateFormat formatDateTime;

  /** Value of entry cannot be greater than power (maxValue * time since last entry). */
  private static final int maxValue = 20000;

  /** Value of entry cannot be greater than power (maxValue * time since last entry). */
  private double maxPower;

  /**
   * Creates a new NotGreaterThanValue object.
   * 
   * @param client Used to grab sensor data from the WattDepot server to use for validation.
   */
  public NotGreaterThanPower(WattDepotClient client) {
    this.client = client;
    this.formatDateTime = new SimpleDateFormat("hh/MM/yyyy hh:mm:ss a", Locale.US);
  }

  /**
   * Given a timestamp, this validator checks to make sure that the reading at that timestamp
   * is not greater than the maximum power that is allowed.
   * 
   * @param entry The entry to validate.
   * @return True if the entry at the current timestamp is less than or equal to the maximum
   * power, false otherwise.
   */
  @Override
  public boolean validateEntry(Object entry) {
    // Unpack
    String sourceName = ((Entry) entry).getSourceName();
    XMLGregorianCalendar currTimestamp = ((Entry) entry).getTimestamp();

    XMLGregorianCalendar prevTimestamp = Tstamp.incrementDays(currTimestamp, -1);
    try {
      List<SensorData> sensorDatas =
          client.getSensorDatas(sourceName, prevTimestamp, currTimestamp);
      if (sensorDatas.size() == 1) {
        // No other data exist but the data at the current timestamp, so return.
        return true;
      }

      SensorData previousData = sensorDatas.get(sensorDatas.size() - 2);
      this.currentData = client.getSensorData(sourceName, currTimestamp);
      int currentReading = Integer.parseInt(this.currentData.getProperty("reading"));
      long prevTimeInMillis =
          previousData.getTimestamp().toGregorianCalendar().getTime().getTime();
      long currTimeInMillis =
          this.currentData.getTimestamp().toGregorianCalendar().getTime().getTime();
      double timeSinceLastEntry = (currTimeInMillis - prevTimeInMillis) / 1000.0 / 60.0 / 60.0;
      /*
       * System.out.println("Previous timestamp    : " + this.previousData.getTimestamp());
       * System.out.println("Current timestamp     : " + this.currentData.getTimestamp() + "\n");
       * System.out.println("Reading               : " + currentReading + " W");
       * System.out.println("Time Since Last Entry : " + String.format("%.2f hours",
       ***** timeSinceLastEntry)); */
      maxPower = maxValue * timeSinceLastEntry;
      /* System.out.print("Power                 : " + String.format("%.2f Wh", maxPower)); */
      /* System.out.println(); */
      if (currentReading <= maxPower) {
        return true;
      }
      else {
        System.err.println(this.getErrorMessage());
      }
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
    }
    catch (NumberFormatException e) {
      System.err.println("Reading at " + this.currentData.getTimestamp()
          + " is not in the correct number format.");
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
    String msg = "";
    try {
      msg = "The data at " + this.currentData.getTimestamp() + " (";
      msg += Integer.parseInt(this.currentData.getProperty("reading")) + " W) cannot be greater ";
      msg += "than " + String.format("%.2f", maxPower) + " Wh.";
    }
    catch (NumberFormatException e) {
      System.err.println("Reading at " + this.currentData.getTimestamp()
          + " is not in the correct number format.");
    }
    return msg;
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
      NotGreaterThanPower validator = new NotGreaterThanPower(client);
      XMLGregorianCalendar currTimestamp = null;
      try {
        Date timestamp = validator.formatDateTime.parse("1/12/2011 4:48:58 PM");
        currTimestamp = Tstamp.makeTimestamp(timestamp.getTime());
      }
      catch (ParseException e) {
        e.printStackTrace();
        System.exit(1);
      }
      Entry currentData = new Entry("994101147077", null, currTimestamp);
      validator.validateEntry(currentData);
    }
    else {
      System.err.println("Unable to connect to WattDepot server.");
    }
  }

}
