package org.wattdepot.hnei.csvexport;

import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used by the HneiExporter class to grab daily data about a particular source.
 * 
 * @author BJ Peter DeLaCruz
 */
public class DailySensorData {

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
   * Given a source and a timestamp, this method returns the daily data about a source.
   * 
   * @param sourceName Name of a source.
   * @param tstamp Timestamp at which to grab data about the source.
   * @return A SensorData object.
   */
  public SensorData getSensorData(String sourceName, String tstamp) {
    XMLGregorianCalendar startTimestamp = null;
    XMLGregorianCalendar endTimestamp = null;
    List<SensorData> data = null;
    SensorData datum = null;
    try {
      Date date = this.formatDate.parse(tstamp);
      startTimestamp = Tstamp.makeTimestamp(date.getTime());
      endTimestamp = Tstamp.incrementSeconds(Tstamp.incrementDays(startTimestamp, 1), -1);
      data = this.client.getSensorDatas(sourceName, startTimestamp, endTimestamp);
      for (SensorData d : data) {
        if (d.getProperty("daily") != null && d.getProperty("daily").equals("true")) {
          datum = d;
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return datum;
  }
}
