package org.wattdepot.hnei.export.cli;

import java.util.Date;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used by the HneiExporter class to grab daily data for a particular source.
 * 
 * @author BJ Peter DeLaCruz
 */
public class DailySensorData implements Retriever {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /** Formats dates that are in the format MM/DD/YYYY. */
  private SimpleDateFormat formatDate;

  /**
   * Creates a new DailySensorData object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public DailySensorData(WattDepotClient client) {
    this.client = client;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
  }

  /**
   * Displays daily data for a source at the given timestamp.
   * 
   * @param sourceName Name of a source.
   * @param tstamp Timestamp at which to grab data for the source.
   * @param option Option to display type of data (energy or power).
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean getSensorData(String sourceName, String tstamp, String option) {
    XMLGregorianCalendar startTimestamp = null;
    XMLGregorianCalendar endTimestamp = null;
    List<SensorData> results = null;
    Date date = null;
    try {
      date = this.formatDate.parse(tstamp);
    }
    catch (ParseException e) {
      e.printStackTrace();
      return false;
    }
    startTimestamp = Tstamp.makeTimestamp(date.getTime());
    endTimestamp = Tstamp.incrementSeconds(Tstamp.incrementDays(startTimestamp, 1), -1);
    try {
      results = this.client.getSensorDatas(sourceName, startTimestamp, endTimestamp);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }
    if (results.isEmpty()) {
      System.out.println("No data exists for source " + sourceName + " on " + startTimestamp + ".");
    }
    else {
      String result = null;
      System.out.println("Reading      Timestamp");
      System.out.println("===============================");
      for (SensorData d : results) {
        if (d.getProperty("daily") != null && d.getProperty("daily").equals("true")) {
          if ("total_energy".equals(option)) {
            System.out.print(String.format("%7.0f",
                d.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE)));
          }
          else if ("energy".equals(option)) {
            System.out.print(String.format("%7.0f",
                d.getPropertyAsDouble(SensorData.ENERGY_CONSUMED)));
          }
          else if ("power".equals(option)) {
            System.out.print(String.format("%7.0f",
                d.getPropertyAsDouble(SensorData.POWER_CONSUMED)));
          }
          System.out.println(result + "     " + d.getTimestamp());
        }
      }
    }
    return true;
  }

  /**
   * Gets a help message for the daily command.
   * 
   * @return A help message.
   */
  @Override
  public String getHelp() {
    String msg = ">> daily [source] [day] [total_energy|energy|power]";
    msg += "\nRetrieves daily energy/power data for a source ";
    msg += "at the given day (hh/DD/yyyy, e.g. 1/20/2011).\n\n";
    return msg;
  }

}
