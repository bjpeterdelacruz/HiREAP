package org.wattdepot.hnei.csvexport;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used by the HneiExporter class to grab hourly data about a particular source.
 * 
 * @author BJ Peter DeLaCruz
 */
public class HourlySensorData {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /* Formats dates that are in the format MM/DD/YYYY hh/mm/ss (A.M.|P.M.). */
  // private SimpleDateFormat formatDateTime;

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
    // this.formatDateTime = new SimpleDateFormat("MM/dd/yyyy hh:mm:ss a", Locale.US);
  }

  /**
   * Returns a list of hourly data about a source given a timestamp with just the date and no time.
   * 
   * @param sourceName Name of a source.
   * @param tstamp Timestamp at which to which to grab data about the source.
   * @return A list of SensorData objects.
   */
  public List<SensorData> getSensorDatas(String sourceName, String tstamp) {
    XMLGregorianCalendar startTimestamp = null;
    XMLGregorianCalendar endTimestamp = null;
    List<SensorData> data = new ArrayList<SensorData>();
    List<SensorData> results = null;
    try {
      Date date = this.formatDate.parse(tstamp);
      startTimestamp = Tstamp.makeTimestamp(date.getTime());
      endTimestamp = Tstamp.incrementSeconds(Tstamp.incrementDays(startTimestamp, 1), -1);
      results = this.client.getSensorDatas(sourceName, startTimestamp, endTimestamp);
      for (SensorData d : results) {
        if (d.getProperty("hourly") != null && d.getProperty("hourly").equals("true")) {
          data.add(d);
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return data;
  }

}
