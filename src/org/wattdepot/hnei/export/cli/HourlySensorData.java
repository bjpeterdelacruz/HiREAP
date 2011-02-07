package org.wattdepot.hnei.export.cli;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used by the HneiExporter class to grab hourly data for a particular source.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HourlySensorData implements Retriever {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /** Formats dates that are in the format MM/DD/YYYY. */
  private SimpleDateFormat formatDate;

  /**
   * Creates a new HourlySensorData object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public HourlySensorData(WattDepotClient client) {
    this.client = client;
    this.formatDate = new SimpleDateFormat("MM/dd/yyyy", Locale.US);
  }

  /**
   * Displays a list of hourly data for a source at the given timestamp.
   * 
   * @param sourceName Name of a source.
   * @param tstamp Timestamp at which to which to grab data for the source.
   */
  @Override
  public void getSensorData(String sourceName, String tstamp) {
    XMLGregorianCalendar startTimestamp = null;
    XMLGregorianCalendar endTimestamp = null;
    List<SensorData> results = null;
    try {
      Date date = this.formatDate.parse(tstamp);
      startTimestamp = Tstamp.makeTimestamp(date.getTime());
      endTimestamp = Tstamp.incrementSeconds(Tstamp.incrementDays(startTimestamp, 1), -1);
      results = this.client.getSensorDatas(sourceName, startTimestamp, endTimestamp);
      if (results.isEmpty()) {
        System.out.println("No data exists for source " + sourceName + " on " + startTimestamp
            + ".");
      }
      else {
        System.out.println("Reading     Timestamp");
        System.out.println("===============================");
        for (SensorData d : results) {
          if (d.getProperty("hourly") != null && d.getProperty("hourly").equals("true")) {
            System.out.print(String.format("%7d", Integer.parseInt(d.getProperty("reading"))));
            System.out.println("     " + d.getTimestamp());
          }
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * Gets a help message for the hourly command.
   * 
   * @return A help message.
   */
  @Override
  public String getHelp() {
    String msg = ">> hourly [source] [day]\nRetrieves all hourly data for a source ";
    msg += "at the given day (hh/DD/yyyy).\n";
    return msg;
  }

}