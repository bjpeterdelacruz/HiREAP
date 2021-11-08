package org.wattdepot.hnei.export.cli;

import org.wattdepot.resource.sensordata.jaxb.SensorData;

/**
 * Classes that implement this interface will display data for a particular source.
 * 
 * @author BJ Peter DeLaCruz
 */
public abstract class Retriever {

  /**
   * Returns a formatted timestamp.
   * 
   * @param data SensorData object with timestamp.
   * @return A formatted timestamp.
   */
  public String getTime(SensorData data) {
    String msg = "";

    int hour = data.getTimestamp().getHour();
    msg += (hour < 10) ? "0" + hour : hour;
    msg += ":";

    int min = data.getTimestamp().getMinute();
    msg += (min < 10) ? "0" + min : min;
    msg += ":";

    int sec = data.getTimestamp().getSecond();
    msg += (sec < 10) ? "0" + sec : sec;

    return msg;
  }

  /**
   * Displays data for a particular source at the given timestamp.
   * 
   * @param sourceName Name of a source.
   * @param start Start timestamp.
   * @param end End timestamp; not used by some classes.
   * @param option Option to display type of data (energy or power).
   * @return True if successful, false otherwise.
   */
  public abstract boolean getSensorData(String sourceName, String start, String end, String option);

  /**
   * Displays a help message for a particular command.
   * 
   * @return A help message.
   */
  public abstract String getHelp();

}
