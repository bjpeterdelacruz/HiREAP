package org.wattdepot.hnei.export.cli;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import javax.xml.datatype.XMLGregorianCalendar;
import org.wattdepot.client.WattDepotClient;
import org.wattdepot.client.WattDepotClientException;
import org.wattdepot.resource.sensordata.jaxb.SensorData;
import org.wattdepot.util.tstamp.Tstamp;

/**
 * This class is used by the HneiWattDepotCli class to get interpolated energy data.
 * 
 * @author BJ Peter DeLaCruz
 */
public class InterpolatedSensorData extends Retriever {

  /** Used to fetch sensor data from the WattDepot server. */
  private WattDepotClient client;

  /** Formats dates that are in the format MM/DD/YYYY. */
  private SimpleDateFormat formatDate;

  /**
   * Creates a new InterpolatedSensorData object.
   * 
   * @param client Used to connect to WattDepot server.
   */
  public InterpolatedSensorData(WattDepotClient client) {
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

    XMLGregorianCalendar startTimestamp = Tstamp.makeTimestamp(date1.getTime());
    XMLGregorianCalendar endTimestamp = Tstamp.makeTimestamp(date2.getTime());

    SensorData result = null;
    double energyConsumed = 0.0;
    long energy = 0;
    try {
      result = this.client.getEnergy(sourceName, startTimestamp, endTimestamp, 0);
      energyConsumed = this.client.getEnergyConsumed(sourceName, startTimestamp, endTimestamp, 0);
      energy = Math.round(energyConsumed);
    }
    catch (WattDepotClientException e) {
      e.printStackTrace();
      return false;
    }

    String msg = result.getTimestamp().getMonth() + "/" + result.getTimestamp().getDay();
    msg += "/" + result.getTimestamp().getYear() + " ";
    msg += this.getTime(result) + " -- " + energy + " Wh\n";

    System.out.print(msg);

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
