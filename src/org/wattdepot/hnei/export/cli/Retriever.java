package org.wattdepot.hnei.export.cli;

/**
 * Classes that implement this interface will display data for a particular source.
 * 
 * @author BJ Peter DeLaCruz
 */
public interface Retriever {

  /**
   * Displays data for a particular source at the given timestamp.
   * 
   * @param sourceName Name of a source.
   * @param tstamp Timestamp at which to grab data for the source.
   * @param option Option to display type of data (energy or power).
   * @return True if successful, false otherwise.
   */
  public boolean getSensorData(String sourceName, String tstamp, String option);

  /**
   * Displays a help message for a particular command.
   * 
   * @return A help message.
   */
  public String getHelp();

}
