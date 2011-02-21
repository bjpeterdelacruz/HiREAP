package org.wattdepot.hnei.export.cli;

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
 * This class is used by the HneiExporter class to grab all energy/power data for a particular
 * source at a given interval.
 * 
 * @author BJ Peter DeLaCruz
 */
public class SourceSensorDatas implements Retriever {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /** Formats dates that are in the format MM/DD/YYYY. */
  private SimpleDateFormat formatDate;

  /**
   * Creates a new SourceSensorDatas object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public SourceSensorDatas(WattDepotClient client) {
    this.client = client;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
  }

  /**
   * Displays a list of energy/power data for a source at the given interval.
   * 
   * @param sourceName Name of a source.
   * @param start Start timestamp.
   * @param end End timestamp.
   * @param option Option to display type of data (energy or power).
   * @return True if successful, false otherwise.
   */
  @Override
  public boolean getSensorData(String sourceName, String start, String end, String option) {
    XMLGregorianCalendar startTimestamp = null;
    XMLGregorianCalendar endTimestamp = null;
    List<SensorData> results = null;
    Date date1 = null;
    Date date2 = null;
    try {
      date1 = this.formatDate.parse(start);
      date2 = this.formatDate.parse(end);
    }
    catch (ParseException e) {
      e.printStackTrace();
      return false;
    }
    startTimestamp = Tstamp.makeTimestamp(date1.getTime());
    endTimestamp = Tstamp.makeTimestamp(date2.getTime());
    endTimestamp = Tstamp.incrementDays(Tstamp.incrementSeconds(endTimestamp, -1), 1);
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
      System.out.println("Timestamp");
      System.out.println("=============================");
      for (SensorData d : results) {
        System.out.print(d.getTimestamp() + "     ");
        if ("total_energy".equals(option)) {
          System.out.println(String.format("%7.0f Wh",
              d.getPropertyAsDouble(SensorData.ENERGY_CONSUMED_TO_DATE)));
        }
        else if ("energy".equals(option)) {
          System.out.println(String.format("%8.0f Wh",
              d.getPropertyAsDouble(SensorData.ENERGY_CONSUMED)));
        }
        else if ("power".equals(option)) {
          System.out.println(String.format("%8.0f W",
              d.getPropertyAsDouble(SensorData.POWER_CONSUMED)));
        }
      }
    }
    return true;
  }

  /**
   * Gets a help message for the all_data command.
   * 
   * @return A help message.
   */
  @Override
  public String getHelp() {
    String msg = ">> all_data [source] [start] [end] [total_energy|energy|power]\n";
    msg += "Retrieves all energy/power data ";
    msg += "for a source at the given interval (hh/DD/yyyy).\n\n";
    return msg;
  }

}