package org.wattdepot.hnei.csvexport;

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
   */
  public void getSensorData(String sourceName, String tstamp);

  /**
   * Displays a help message for a particular command.
   * 
   * @return A help message.
   */
  public String getHelp();

}
